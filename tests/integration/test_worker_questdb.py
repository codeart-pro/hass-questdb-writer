"""End-to-end worker delivery tests against a real QuestDB server."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import os
from pathlib import Path
import socket
import tempfile
from threading import Thread
import time
import unittest
import urllib.parse
import urllib.request

from custom_components.hass_questdb_writer.event import EventEnvelope
from custom_components.hass_questdb_writer.schema import IlpSchemaManager
from custom_components.hass_questdb_writer.spool import SQLiteSpool
from custom_components.hass_questdb_writer.transport import IlpHttpTransport
from custom_components.hass_questdb_writer.worker import (
    WorkerSettings,
    WorkerState,
    WriterService,
)


def proxy_handler(
    upstream_host: str, upstream_port: int
) -> type[BaseHTTPRequestHandler]:
    """Create a quiet HTTP handler forwarding writes to real QuestDB."""

    class QuestDbProxyHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _forward(self, method: str) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            connection = http.client.HTTPConnection(
                upstream_host, upstream_port, timeout=5
            )
            try:
                headers = (
                    {"Content-Type": "text/plain; charset=utf-8"}
                    if method == "POST"
                    else {}
                )
                connection.request(
                    method,
                    self.path,
                    body=body or None,
                    headers=headers,
                )
                response = connection.getresponse()
                response_body = response.read()
            finally:
                connection.close()
            self.send_response(response.status)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        def do_POST(self) -> None:
            self._forward("POST")

        def do_GET(self) -> None:
            self._forward("GET")

        def log_message(self, format: str, *args: object) -> None:
            return

    return QuestDbProxyHandler


class WorkerQuestDbIntegrationTests(unittest.TestCase):
    host = os.environ.get("QUESTDB_HTTP_HOST", "questdb")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    table = "hass_qdb_writer_worker_integration"

    def sql(self, statement: str) -> dict:
        query = urllib.parse.urlencode({"query": statement})
        with urllib.request.urlopen(
            f"http://{self.host}:{self.port}/exec?{query}", timeout=10
        ) as response:
            return json.load(response)

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.spool_path = Path(self.temporary_directory.name) / "worker.db"
        # The table is owned by the integration: the worker's schema manager
        # creates and validates it on start, so no manual DDL here.
        self.sql(f"drop table if exists {self.table}")

    def tearDown(self) -> None:
        self.sql(f"drop table if exists {self.table}")

    def spool_factory(self) -> SQLiteSpool:
        return SQLiteSpool(
            self.spool_path,
            max_pending_rows=1_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=100_000,
            max_dead_letter_rows=100,
            max_dead_letter_bytes=1_000_000,
            busy_timeout_seconds=1,
        )

    def event(self, index: int) -> EventEnvelope:
        timestamp = 1_700_000_000_123_456_000 + index * 1_000
        return EventEnvelope(
            event_id=f"worker-event-{index}",
            entity_id=f"sensor.worker_{index}",
            state=f"state-{index}",
            attributes_json='{"source":"integration"}',
            ingested_at_ns=timestamp + 2_000,
            last_changed_ns=timestamp - 2_000,
            last_updated_ns=timestamp - 1_000,
            context_id=f"context-{index}",
        )

    def test_worker_persists_batches_and_confirms_real_rows(self) -> None:
        settings = WorkerSettings(
            ingress_queue_capacity=20,
            max_serialized_event_bytes=10_000,
            persist_batch_rows=20,
            delivery_batch_rows=3,
            delivery_batch_bytes=100_000,
            flush_interval_seconds=0.05,
            persist_idle_poll_seconds=0.05,
            retry_initial_seconds=0.05,
            retry_max_seconds=0.2,
            retry_multiplier=2,
            retry_jitter_ratio=0,
            flush_on_shutdown=False,
        )
        service = WriterService(
            table=self.table,
            settings=settings,
            spool_factory=self.spool_factory,
            transport_factory=lambda: IlpHttpTransport(
                self.host,
                self.port,
                use_tls=False,
                timeout_seconds=2,
            ),
            schema_factory=lambda: IlpSchemaManager(
                self.host,
                self.port,
                use_tls=False,
                timeout_seconds=2,
            ),
            random_source=lambda: 0.5,
        )
        service.start(timeout_seconds=2)
        try:
            for index in (1, 2, 3):
                self.assertTrue(service.submit(self.event(index)))
            deadline = time.monotonic() + 5
            while service.snapshot().delivered_events != 3:
                if time.monotonic() >= deadline:
                    self.fail(f"delivery timeout: {service.snapshot()}")
                time.sleep(0.01)
        finally:
            self.assertTrue(service.stop(timeout_seconds=2))

        query = (
            f"select entity_id, domain, event_id, state, attributes, "
            f"ingested_at, last_changed, last_updated, context_id "
            f"from {self.table} order by last_updated"
        )
        visibility_deadline = time.monotonic() + 5
        while True:
            result = self.sql(query)
            if result["count"] == 3 or time.monotonic() >= visibility_deadline:
                break
            time.sleep(0.01)
        self.assertEqual(result["count"], 3)
        first = result["dataset"][0]
        self.assertEqual(first[0], "sensor.worker_1")
        self.assertEqual(first[1], "sensor")
        self.assertEqual(first[2], "worker-event-1")
        self.assertEqual(first[3], "state-1")
        self.assertEqual(first[4], '{"source":"integration"}')
        self.assertEqual(first[5], "2023-11-14T22:13:20.123459Z")
        self.assertEqual(first[6], "2023-11-14T22:13:20.123455Z")
        self.assertEqual(first[7], "2023-11-14T22:13:20.123456Z")
        self.assertEqual(first[8], "context-1")

        with self.spool_factory() as spool:
            self.assertEqual(spool.stats().pending_rows, 0)
            self.assertEqual(spool.stats().dead_letter_rows, 0)

    def test_worker_recovers_after_real_connection_refusal(self) -> None:
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        proxy_port = probe.getsockname()[1]
        probe.close()

        settings = WorkerSettings(
            ingress_queue_capacity=10,
            max_serialized_event_bytes=10_000,
            persist_batch_rows=10,
            delivery_batch_rows=1,
            delivery_batch_bytes=100_000,
            flush_interval_seconds=0.01,
            persist_idle_poll_seconds=0.05,
            retry_initial_seconds=0.05,
            retry_max_seconds=0.1,
            retry_multiplier=2,
            retry_jitter_ratio=0,
            flush_on_shutdown=False,
        )
        service = WriterService(
            table=self.table,
            settings=settings,
            spool_factory=self.spool_factory,
            transport_factory=lambda: IlpHttpTransport(
                "127.0.0.1",
                proxy_port,
                use_tls=False,
                timeout_seconds=1,
            ),
            schema_factory=lambda: IlpSchemaManager(
                "127.0.0.1",
                proxy_port,
                use_tls=False,
                timeout_seconds=1,
            ),
            random_source=lambda: 0.5,
        )
        service.start(timeout_seconds=2)
        self.assertTrue(service.submit(self.event(1)))
        retry_deadline = time.monotonic() + 2
        while service.snapshot().state is not WorkerState.RETRY_WAIT:
            if time.monotonic() >= retry_deadline:
                self.fail(f"worker did not enter retry: {service.snapshot()}")
            time.sleep(0.005)
        self.assertEqual(service.snapshot().pending_rows, 1)
        self.assertTrue(service.snapshot().thread_alive)

        server = ThreadingHTTPServer(
            ("127.0.0.1", proxy_port), proxy_handler(self.host, self.port)
        )
        server.daemon_threads = True
        server_thread = Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            delivery_deadline = time.monotonic() + 5
            while service.snapshot().delivered_events != 1:
                if time.monotonic() >= delivery_deadline:
                    self.fail(f"worker did not recover: {service.snapshot()}")
                time.sleep(0.01)
        finally:
            self.assertTrue(service.stop(timeout_seconds=2))
            server.shutdown()
            server.server_close()
            server_thread.join(2)

        self.assertGreaterEqual(service.snapshot().retry_attempts, 1)
        with self.spool_factory() as spool:
            self.assertEqual(spool.stats().pending_rows, 0)


if __name__ == "__main__":
    unittest.main()
