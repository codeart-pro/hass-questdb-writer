"""Tests for the persistent pure-Python ILP/HTTP transport."""

from __future__ import annotations

import base64
import http.client
import unittest

from custom_components.hass_questdb_writer.transport import (
    AuthenticationIlpError,
    IlpHttpTransport,
    MAX_ERROR_BODY_BYTES,
    PermanentIlpError,
    RetryableIlpError,
    WRITE_PATH,
)


class FakeResponse:
    def __init__(self, status: int, body: bytes = b"") -> None:
        self.status = status
        self._body = body

    def read(self, amount: int | None = None) -> bytes:
        if amount is None:
            return self._body
        return self._body[:amount]


class FakeConnection:
    def __init__(self, responses: list[FakeResponse | BaseException]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, str, bytes, dict[str, str]]] = []
        self.closed = False

    def request(
        self, method: str, path: str, body: bytes, headers: dict[str, str]
    ) -> None:
        self.requests.append((method, path, body, headers))

    def getresponse(self) -> FakeResponse:
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def close(self) -> None:
        self.closed = True


class TestTransport(IlpHttpTransport):
    def __init__(self, connections: list[FakeConnection], **kwargs: object) -> None:
        self._test_connections = connections
        self.connection_count = 0
        super().__init__("questdb", 9000, **kwargs)

    def _make_connection(self) -> FakeConnection:
        connection = self._test_connections[self.connection_count]
        self.connection_count += 1
        return connection


class IlpHttpTransportTests(unittest.TestCase):
    def transport(
        self, *connections: FakeConnection, **kwargs: object
    ) -> TestTransport:
        return TestTransport(
            list(connections), use_tls=False, timeout_seconds=5, **kwargs
        )

    def test_reuses_connection_and_sends_expected_request(self) -> None:
        connection = FakeConnection([FakeResponse(204), FakeResponse(200)])
        transport = self.transport(connection)
        transport.send_batch(b"events value=1i 1\n")
        transport.send_batch(b"events value=2i 2\n")

        self.assertEqual(transport.connection_count, 1)
        self.assertEqual(len(connection.requests), 2)
        method, path, body, headers = connection.requests[0]
        self.assertEqual((method, path, body), ("POST", WRITE_PATH, b"events value=1i 1\n"))
        self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")

    def test_adds_basic_auth_header(self) -> None:
        connection = FakeConnection([FakeResponse(204)])
        transport = self.transport(connection, username="user", password="pass")
        transport.send_batch(b"events value=1i 1\n")
        expected = base64.b64encode(b"user:pass").decode("ascii")
        self.assertEqual(
            connection.requests[0][3]["Authorization"], f"Basic {expected}"
        )

    def test_classifies_permanent_request_error(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(400, b"bad row")]))
        with self.assertRaises(PermanentIlpError) as caught:
            transport.send_batch(b"events value=1i 1\n")
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)
        self.assertEqual(caught.exception.status_code, 400)
        self.assertIn("bad row", str(caught.exception))

    def test_classifies_authentication_error(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(401)]))
        with self.assertRaises(AuthenticationIlpError) as caught:
            transport.send_batch(b"events value=1i 1\n")
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)

    def test_classifies_rate_limit_as_retryable_but_not_uncertain(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(429)]))
        with self.assertRaises(RetryableIlpError) as caught:
            transport.send_batch(b"events value=1i 1\n")
        self.assertTrue(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)

    def test_classifies_server_error_as_retryable_and_uncertain(self) -> None:
        connection = FakeConnection([FakeResponse(503)])
        transport = self.transport(connection)
        with self.assertRaises(RetryableIlpError) as caught:
            transport.send_batch(b"events value=1i 1\n")
        self.assertTrue(caught.exception.retryable)
        self.assertTrue(caught.exception.delivery_uncertain)
        self.assertTrue(connection.closed)

    def test_reconnects_after_network_failure(self) -> None:
        failed = FakeConnection([TimeoutError("timed out")])
        recovered = FakeConnection([FakeResponse(204)])
        transport = self.transport(failed, recovered)
        with self.assertRaises(RetryableIlpError) as caught:
            transport.send_batch(b"events value=1i 1\n")
        self.assertTrue(caught.exception.delivery_uncertain)
        self.assertTrue(failed.closed)

        transport.send_batch(b"events value=1i 1\n")
        self.assertEqual(transport.connection_count, 2)

    def test_validates_payload_before_connecting(self) -> None:
        transport = self.transport(FakeConnection([]))
        for payload in (b"", b"events value=1i 1"):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    transport.send_batch(payload)
        self.assertEqual(transport.connection_count, 0)

    def test_close_is_idempotent(self) -> None:
        connection = FakeConnection([FakeResponse(204)])
        transport = self.transport(connection)
        transport.send_batch(b"events value=1i 1\n")
        transport.close()
        transport.close()
        self.assertTrue(connection.closed)

    def test_exec_query_encodes_and_parses_json(self) -> None:
        body = (
            b'{"query":"SHOW COLUMNS FROM ha_events","columns":'
            b'[{"name":"column","type":"STRING"}],"dataset":[["state"]],"count":1}'
        )
        connection = FakeConnection([FakeResponse(200, body)])
        transport = self.transport(connection)
        document = transport.exec_query("SHOW COLUMNS FROM ha_events")
        self.assertEqual(document["dataset"], [["state"]])
        method, path, body_bytes, _ = connection.requests[0]
        self.assertEqual(method, "GET")
        self.assertTrue(path.startswith("/exec?query=SHOW%20COLUMNS%20FROM"))
        self.assertIsNone(body_bytes)

    def test_exec_query_classifies_authentication_error(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(401)]))
        with self.assertRaises(AuthenticationIlpError) as caught:
            transport.exec_query("SELECT 1")
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)

    def test_exec_query_classifies_server_error_as_retryable(self) -> None:
        connection = FakeConnection([FakeResponse(503, b"not ready")])
        transport = self.transport(connection)
        with self.assertRaises(RetryableIlpError) as caught:
            transport.exec_query("SELECT 1")
        self.assertTrue(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)
        self.assertTrue(connection.closed)

    def test_exec_query_classifies_rejected_statement_as_permanent(self) -> None:
        body = b'{"query":"BAD SQL","error":"unexpected token","position":1}'
        transport = self.transport(FakeConnection([FakeResponse(400, body)]))
        with self.assertRaises(PermanentIlpError) as caught:
            transport.exec_query("BAD SQL")
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)
        self.assertIn("unexpected token", str(caught.exception))

    def test_exec_query_network_failure_is_retryable_and_not_uncertain(self) -> None:
        connection = FakeConnection([TimeoutError("timed out")])
        transport = self.transport(connection)
        with self.assertRaises(RetryableIlpError) as caught:
            transport.exec_query("SELECT 1")
        self.assertTrue(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)
        self.assertTrue(connection.closed)

    def test_exec_query_validates_before_connecting(self) -> None:
        transport = self.transport(FakeConnection([]))
        for query in ("", None):
            with self.subTest(query=query):
                with self.assertRaises(ValueError):
                    transport.exec_query(query)  # type: ignore[arg-type]
        self.assertEqual(transport.connection_count, 0)

    def test_constructor_rejects_empty_host(self) -> None:
        with self.assertRaises(ValueError):
            IlpHttpTransport("", 9000, use_tls=False, timeout_seconds=5)

    def test_constructor_rejects_out_of_range_port(self) -> None:
        for port in (0, 70000):
            with self.subTest(port=port):
                with self.assertRaises(ValueError):
                    IlpHttpTransport("q", port, use_tls=False, timeout_seconds=5)

    def test_constructor_rejects_non_positive_timeout(self) -> None:
        with self.assertRaises(ValueError):
            IlpHttpTransport("q", 9000, use_tls=False, timeout_seconds=0)

    def test_constructor_rejects_username_without_password(self) -> None:
        with self.assertRaises(ValueError):
            IlpHttpTransport(
                "q", 9000, use_tls=False, timeout_seconds=5, username="u"
            )

    def test_constructor_rejects_ssl_context_without_tls(self) -> None:
        with self.assertRaises(ValueError):
            IlpHttpTransport(
                "q",
                9000,
                use_tls=False,
                timeout_seconds=5,
                ssl_context=object(),  # type: ignore[arg-type]
            )

    def test_tls_uses_https_connection(self) -> None:
        transport = IlpHttpTransport(
            "q", 9000, use_tls=True, timeout_seconds=5
        )
        connection = transport._make_connection()
        self.assertIsInstance(connection, http.client.HTTPSConnection)

    def test_send_batch_2xx_overflow_closes_connection(self) -> None:
        connection = FakeConnection(
            [FakeResponse(204, b"x" * (MAX_ERROR_BODY_BYTES + 10))]
        )
        transport = self.transport(connection)
        transport.send_batch(b"row\n")
        self.assertTrue(connection.closed)

    def test_send_batch_permanent_error_truncates_body_and_closes(self) -> None:
        connection = FakeConnection(
            [FakeResponse(400, b"x" * (MAX_ERROR_BODY_BYTES + 10))]
        )
        transport = self.transport(connection)
        with self.assertRaises(PermanentIlpError):
            transport.send_batch(b"row\n")
        self.assertTrue(connection.closed)

    def test_send_batch_408_is_retryable_uncertain_and_closes(self) -> None:
        connection = FakeConnection([FakeResponse(408, b"timeout")])
        transport = self.transport(connection)
        with self.assertRaises(RetryableIlpError) as caught:
            transport.send_batch(b"row\n")
        self.assertTrue(caught.exception.retryable)
        self.assertTrue(caught.exception.delivery_uncertain)
        self.assertTrue(connection.closed)

    def test_exec_query_408_is_retryable(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(408, b"x")]))
        with self.assertRaises(RetryableIlpError):
            transport.exec_query("SELECT 1")

    def test_exec_query_5xx_overflow_truncates_and_closes(self) -> None:
        connection = FakeConnection(
            [FakeResponse(500, b"x" * (MAX_ERROR_BODY_BYTES + 10))]
        )
        transport = self.transport(connection)
        with self.assertRaises(RetryableIlpError):
            transport.exec_query("SELECT 1")
        self.assertTrue(connection.closed)

    def test_exec_query_2xx_overflow_closes_connection(self) -> None:
        connection = FakeConnection(
            [FakeResponse(200, b"x" * (MAX_ERROR_BODY_BYTES + 10))]
        )
        transport = self.transport(connection)
        with self.assertRaises(PermanentIlpError):
            transport.exec_query("SELECT 1")
        self.assertTrue(connection.closed)

    def test_exec_query_invalid_json_is_permanent(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(200, b"not json")]))
        with self.assertRaises(PermanentIlpError):
            transport.exec_query("SELECT 1")

    def test_exec_query_non_object_json_is_permanent(self) -> None:
        transport = self.transport(FakeConnection([FakeResponse(200, b"[1,2]")]))
        with self.assertRaises(PermanentIlpError):
            transport.exec_query("SELECT 1")
