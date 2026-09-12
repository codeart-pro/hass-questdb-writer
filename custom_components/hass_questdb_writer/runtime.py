"""Home Assistant lifecycle adapter for the independent writer service."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import datetime as dt
from functools import partial
import logging
import ssl
import math
from pathlib import Path
import time
from typing import Final
from uuid import uuid4

from homeassistant.const import EVENT_STATE_CHANGED, STATE_UNKNOWN
from homeassistant.core import (
    CALLBACK_TYPE,
    Event,
    EventStateChangedData,
    HomeAssistant,
    callback,
)
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.entityfilter import EntityFilter
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.json import json_dumps

from .attribute_filter import AttributeFilter
from .const import DOMAIN
from .event import EventEnvelope, EventEnvelopeError
from .schema import IlpSchemaManager
from .spool import SQLiteSpool
from .transport import IlpHttpTransport
from .worker import WorkerSettings, WorkerSnapshot, WorkerState, WriterService

_LOGGER = logging.getLogger(__name__)
_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


@dataclass(frozen=True, slots=True)
class SpoolConfiguration:
    """Explicit local-storage settings for one config entry."""

    path: Path
    max_pending_rows: int
    max_pending_bytes: int
    max_event_bytes: int
    max_dead_letter_rows: int
    max_dead_letter_bytes: int
    busy_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class ConnectionConfiguration:
    """Explicit QuestDB connection settings for one config entry."""

    host: str
    port: int
    use_tls: bool
    timeout_seconds: float
    username: str | None = None
    password: str | None = None
    tls_self_signed: bool = False


def _tls_context(
    connection: ConnectionConfiguration,
) -> ssl.SSLContext | None:
    """An unverified TLS context for self-signed certificates, or None."""
    if connection.tls_self_signed:
        return ssl._create_unverified_context()
    return None


@dataclass(frozen=True, slots=True)
class RuntimeConfiguration:
    """Complete settings required to own one runtime instance."""

    table: str
    spool: SpoolConfiguration
    connection: ConnectionConfiguration
    worker: WorkerSettings
    start_timeout_seconds: float
    stop_timeout_seconds: float
    tracked_entity_ids: tuple[str, ...] | None
    entity_filter: EntityFilter | None = None
    attribute_filter: AttributeFilter | None = None
    retention_days: int = 0

    def __post_init__(self) -> None:
        if not self.table:
            raise ValueError("table must not be empty")
        for name in ("start_timeout_seconds", "stop_timeout_seconds"):
            value = getattr(self, name)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive and finite")
        if self.tracked_entity_ids is not None:
            if not self.tracked_entity_ids or any(
                not isinstance(entity_id, str) or not entity_id
                for entity_id in self.tracked_entity_ids
            ):
                raise ValueError(
                    "tracked_entity_ids must be None or non-empty entity IDs"
                )


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    """Combined event-listener and worker diagnostics."""

    listener_active: bool
    state_events_seen: int
    state_events_accepted: int
    state_events_without_new_state: int
    state_events_skipped_unknown: int
    state_events_excluded: int
    attribute_entries_removed: int
    conversion_errors: int
    submission_rejections: int
    worker: WorkerSnapshot


def datetime_to_epoch_ns(value: dt.datetime) -> int:
    """Convert an aware datetime to integer epoch nanoseconds."""
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("timestamp must be a timezone-aware datetime")
    delta = value.astimezone(dt.UTC) - _EPOCH
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _log_on_power_of_two(count: int) -> bool:
    """Rate-limit repeated event-path errors without a timer task."""
    return count > 0 and count & (count - 1) == 0


class HassQuestDbRuntime:
    """Own one HA listener and one writer service for a config entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        configuration: RuntimeConfiguration,
        *,
        service_factory: Callable[[], WriterService] | None = None,
        event_id_factory: Callable[[], str] | None = None,
        wall_time_ns: Callable[[], int] = time.time_ns,
        entry_id: str | None = None,
        monitor_interval_seconds: float = 30.0,
        request_reauth: Callable[[], None] | None = None,
    ) -> None:
        self._hass = hass
        self._configuration = configuration
        self._event_id_factory = event_id_factory or (lambda: uuid4().hex)
        self._wall_time_ns = wall_time_ns
        self._service = (
            service_factory() if service_factory is not None else self._make_service()
        )
        self._unsubscribe: CALLBACK_TYPE | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._entry_id = entry_id
        self._monitor_interval_seconds = monitor_interval_seconds
        self._request_reauth = request_reauth
        self._reauth_requested = False
        self._events_seen = 0
        self._events_accepted = 0
        self._events_without_new_state = 0
        self._events_skipped_unknown = 0
        self._events_excluded = 0
        self._attribute_entries_removed = 0
        self._conversion_errors = 0
        self._submission_rejections = 0

    @property
    def service(self) -> WriterService:
        """Return the owned writer for diagnostics and tests."""
        return self._service

    def _make_service(self) -> WriterService:
        configuration = self._configuration

        def spool_factory() -> SQLiteSpool:
            configuration.spool.path.parent.mkdir(parents=True, exist_ok=True)
            return SQLiteSpool(
                configuration.spool.path,
                max_pending_rows=configuration.spool.max_pending_rows,
                max_pending_bytes=configuration.spool.max_pending_bytes,
                max_event_bytes=configuration.spool.max_event_bytes,
                max_dead_letter_rows=configuration.spool.max_dead_letter_rows,
                max_dead_letter_bytes=configuration.spool.max_dead_letter_bytes,
                busy_timeout_seconds=configuration.spool.busy_timeout_seconds,
            )

        def transport_factory() -> IlpHttpTransport:
            return IlpHttpTransport(
                configuration.connection.host,
                configuration.connection.port,
                use_tls=configuration.connection.use_tls,
                timeout_seconds=configuration.connection.timeout_seconds,
                username=configuration.connection.username,
                password=configuration.connection.password,
                ssl_context=_tls_context(configuration.connection),
            )

        def schema_factory() -> IlpSchemaManager:
            return IlpSchemaManager(
                configuration.connection.host,
                configuration.connection.port,
                use_tls=configuration.connection.use_tls,
                timeout_seconds=configuration.connection.timeout_seconds,
                username=configuration.connection.username,
                password=configuration.connection.password,
                ssl_context=_tls_context(configuration.connection),
                retention_days=configuration.retention_days,
            )

        return WriterService(
            table=configuration.table,
            settings=configuration.worker,
            spool_factory=spool_factory,
            transport_factory=transport_factory,
            schema_factory=schema_factory,
        )

    async def async_start(self) -> None:
        """Start the worker before registering exactly one listener."""
        try:
            await self._hass.async_add_executor_job(
                partial(
                    self._service.start,
                    timeout_seconds=self._configuration.start_timeout_seconds,
                )
            )
            entity_ids = self._configuration.tracked_entity_ids
            if entity_ids is None:
                self._unsubscribe = self._hass.bus.async_listen(
                    EVENT_STATE_CHANGED, self._async_state_changed
                )
            else:
                self._unsubscribe = async_track_state_change_event(
                    self._hass, entity_ids, self._async_state_changed
                )
            if self._entry_id is not None:
                self._sync_auth_issue()
                self._monitor_task = self._hass.async_create_background_task(
                    self._monitor_loop(), "hass_questdb_writer_auth_monitor"
                )
        except BaseException:
            try:
                stopped = await self._hass.async_add_executor_job(
                    partial(
                        self._service.stop,
                        timeout_seconds=(
                            self._configuration.stop_timeout_seconds
                        ),
                    )
                )
                if not stopped:
                    _LOGGER.error(
                        "Writer did not stop after config-entry setup failed"
                    )
            except Exception:
                _LOGGER.exception(
                    "Could not stop writer after config-entry setup failed"
                )
            raise

    def _issue_id(self) -> str:
        return f"auth_failed_{self._entry_id}"

    def _sync_auth_issue(self) -> None:
        """Create or remove the credentials repair issue for this entry.

        A rejected credential set is also offered for repair through the
        reauthentication flow, once per outage (ADR-0012).
        """
        if self._entry_id is None:
            return
        snapshot = self._service.snapshot()
        if (
            snapshot.state == WorkerState.BLOCKED
            and snapshot.block_reason == "auth"
        ):
            ir.async_create_issue(
                self._hass,
                DOMAIN,
                self._issue_id(),
                is_fixable=False,
                is_persistent=True,
                severity=ir.IssueSeverity.ERROR,
                translation_key="auth_failed",
                translation_placeholders={
                    "host": str(self._configuration.connection.host)
                },
            )
            self._request_reauth_once()
        else:
            ir.async_delete_issue(self._hass, DOMAIN, self._issue_id())
            self._reauth_requested = False

    def _request_reauth_once(self) -> None:
        """Ask Home Assistant to start a reauthentication flow, once per outage."""
        if self._reauth_requested or self._request_reauth is None:
            return
        self._reauth_requested = True
        self._request_reauth()

    async def _monitor_loop(self) -> None:
        """Periodically reflect the worker auth state in the issue registry."""
        while True:
            await asyncio.sleep(self._monitor_interval_seconds)
            try:
                self._sync_auth_issue()
            except Exception:
                _LOGGER.exception("Auth issue sync failed")

    async def async_stop(self) -> bool:
        """Unsubscribe first, then stop and join the worker."""
        monitor_task = self._monitor_task
        if monitor_task is not None:
            monitor_task.cancel()
            self._monitor_task = None
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        unsubscribe = self._unsubscribe
        unsubscribed = True
        if unsubscribe is not None:
            try:
                unsubscribe()
            except Exception:
                unsubscribed = False
                _LOGGER.exception("Could not remove the state-change listener")
            else:
                self._unsubscribe = None
        stopped = await self._hass.async_add_executor_job(
            partial(
                self._service.stop,
                timeout_seconds=self._configuration.stop_timeout_seconds,
            )
        )
        if self._entry_id is not None:
            ir.async_delete_issue(self._hass, DOMAIN, self._issue_id())
        return unsubscribed and stopped

    @callback
    def _async_state_changed(
        self, event: Event[EventStateChangedData]
    ) -> None:
        self._events_seen += 1
        new_state = event.data["new_state"]
        if new_state is None:
            self._events_without_new_state += 1
            return
        entity_filter = self._configuration.entity_filter
        if entity_filter is not None and not entity_filter(new_state.entity_id):
            self._events_excluded += 1
            return
        if new_state.state == STATE_UNKNOWN:
            self._events_skipped_unknown += 1
            return
        attributes = dict(new_state.attributes)
        attribute_filter = self._configuration.attribute_filter
        if attribute_filter is not None:
            kept = {
                name: value
                for name, value in attributes.items()
                if attribute_filter(name)
            }
            self._attribute_entries_removed += len(attributes) - len(kept)
            attributes = kept
        try:
            envelope = EventEnvelope(
                event_id=self._event_id_factory(),
                entity_id=new_state.entity_id,
                state=new_state.state,
                attributes_json=json_dumps(attributes),
                ingested_at_ns=self._wall_time_ns(),
                last_changed_ns=datetime_to_epoch_ns(new_state.last_changed),
                last_updated_ns=datetime_to_epoch_ns(new_state.last_updated),
                context_id=new_state.context.id,
            )
        except (EventEnvelopeError, TypeError, ValueError, OverflowError) as exc:
            self._conversion_errors += 1
            if _log_on_power_of_two(self._conversion_errors):
                _LOGGER.error(
                    "Could not convert state event for %s (%d failures): %s",
                    new_state.entity_id,
                    self._conversion_errors,
                    exc,
                )
            return

        if self._service.submit(envelope):
            self._events_accepted += 1
            return
        self._submission_rejections += 1
        if _log_on_power_of_two(self._submission_rejections):
            _LOGGER.error(
                "Writer rejected state event for %s (%d rejections): %s",
                new_state.entity_id,
                self._submission_rejections,
                self._service.snapshot().last_error,
            )

    def snapshot(self) -> RuntimeSnapshot:
        """Return listener and worker health without blocking the HA loop."""
        return RuntimeSnapshot(
            listener_active=self._unsubscribe is not None,
            state_events_seen=self._events_seen,
            state_events_accepted=self._events_accepted,
            state_events_without_new_state=self._events_without_new_state,
            state_events_skipped_unknown=self._events_skipped_unknown,
            state_events_excluded=self._events_excluded,
            attribute_entries_removed=self._attribute_entries_removed,
            conversion_errors=self._conversion_errors,
            submission_rejections=self._submission_rejections,
            worker=self._service.snapshot(),
        )
