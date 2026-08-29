"""Constants for HASS QuestDB Writer."""

DOMAIN = "hass_questdb_writer"

CONF_HOST = "host"
CONF_PORT = "port"
CONF_TABLE = "table"
CONF_USE_TLS = "use_tls"
CONF_USERNAME = "username"
CONF_PASSWORD = "password"

CONF_SHOW_ADVANCED = "show_advanced"
CONF_INCLUDE = "include"
CONF_EXCLUDE = "exclude"

# Options-flow keys; every value defaults to the matching PROVISIONAL_*
# constant so entries created before an option existed keep working.
CONF_INGRESS_QUEUE_CAPACITY = "ingress_queue_capacity"
CONF_MAX_SERIALIZED_EVENT_BYTES = "max_serialized_event_bytes"
CONF_PERSIST_BATCH_ROWS = "persist_batch_rows"
CONF_DELIVERY_BATCH_ROWS = "delivery_batch_rows"
CONF_DELIVERY_BATCH_BYTES = "delivery_batch_bytes"
CONF_FLUSH_INTERVAL_SECONDS = "flush_interval_seconds"
CONF_RETRY_INITIAL_SECONDS = "retry_initial_seconds"
CONF_RETRY_MAX_SECONDS = "retry_max_seconds"
CONF_RETRY_MULTIPLIER = "retry_multiplier"
CONF_RETRY_JITTER_RATIO = "retry_jitter_ratio"
CONF_FLUSH_ON_SHUTDOWN = "flush_on_shutdown"
CONF_MAX_PENDING_ROWS = "max_pending_rows"
CONF_MAX_PENDING_BYTES = "max_pending_bytes"
CONF_MAX_DEAD_LETTER_ROWS = "max_dead_letter_rows"
CONF_MAX_DEAD_LETTER_BYTES = "max_dead_letter_bytes"
CONF_SQLITE_BUSY_TIMEOUT_SECONDS = "sqlite_busy_timeout_seconds"
CONF_HTTP_TIMEOUT_SECONDS = "http_timeout_seconds"
CONF_START_TIMEOUT_SECONDS = "start_timeout_seconds"
CONF_STOP_TIMEOUT_SECONDS = "stop_timeout_seconds"

DEFAULT_PORT = 9000
DEFAULT_TABLE = "hass_questdb_writer_events"

# Development profile only. These are explicit and intentionally named
# provisional until production event-rate, payload-size, disk, and outage tests
# select supported defaults.
PROVISIONAL_INGRESS_QUEUE_CAPACITY = 1_000
PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES = 64 * 1_024
PROVISIONAL_PERSIST_BATCH_ROWS = 100
PROVISIONAL_DELIVERY_BATCH_ROWS = 1_000
PROVISIONAL_DELIVERY_BATCH_BYTES = 512 * 1_024
PROVISIONAL_FLUSH_INTERVAL_SECONDS = 1.0
PROVISIONAL_RETRY_INITIAL_SECONDS = 1.0
PROVISIONAL_RETRY_MAX_SECONDS = 60.0
PROVISIONAL_RETRY_MULTIPLIER = 2.0
PROVISIONAL_RETRY_JITTER_RATIO = 0.2
PROVISIONAL_FLUSH_ON_SHUTDOWN = False

PROVISIONAL_MAX_PENDING_ROWS = 100_000
PROVISIONAL_MAX_PENDING_BYTES = 64 * 1_024 * 1_024
PROVISIONAL_MAX_DEAD_LETTER_ROWS = 1_000
PROVISIONAL_MAX_DEAD_LETTER_BYTES = 16 * 1_024 * 1_024
PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS = 1.0

PROVISIONAL_HTTP_TIMEOUT_SECONDS = 10.0
PROVISIONAL_START_TIMEOUT_SECONDS = 10.0
PROVISIONAL_STOP_TIMEOUT_SECONDS = 15.0
