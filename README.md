# HASS QuestDB Writer

Isolated development project for the HASS QuestDB Writer integration.

The target design is documented in [docs/architecture.md](docs/architecture.md).

## Safety boundary

The default development configuration only connects to the local QuestDB container.
It contains no production credentials or production endpoints.

## Baseline versions

- Home Assistant: `2026.7.2`
- QuestDB server: `10.0.1`
- Baseline-only QuestDB client: `4.1.0` in the temporary QSS fixture
- Target runtime: pure-Python ILP/HTTP with no native QuestDB client dependency

## Local endpoints

- Home Assistant: <http://localhost:18123>
- QuestDB Web Console / HTTP: <http://localhost:19000>
- QuestDB TCP/ILP: `localhost:19009`
- QuestDB health endpoint: <http://localhost:19003>

## Local test credentials

These credentials belong only to the isolated Podman development stack. They
are intentionally weak and must not be reused for production or for a Home
Assistant instance reachable from another network.

| Service | Username | Password | Access |
|---|---|---|---|
| Home Assistant | `test` | `test` | Local administrator |
| QuestDB HTTP/ILP and Web Console | none | none | Authentication disabled |

QuestDB has no configured username or password because authentication is not
enabled in `compose.yaml`. There are no other passwords or production
credentials in this repository. Production secrets must not be committed.

## Commands

```text
make validate
make up
make status
make logs
make down
```

The reproducible SQLite spool benchmark runs from the repository root as:

```text
python -m benchmarks.sqlite_spool
```

The runtime integration is `custom_components/hass_questdb_writer`. It is a
clean implementation and does not use QSS as its code base.

The current `0.1.0-dev0` config-entry runtime is wired to the worker and uses an
explicit provisional local profile. Queue, spool, retry, and timeout values are
not production defaults yet; see
[`docs/decisions/0004-home-assistant-lifecycle.md`](docs/decisions/0004-home-assistant-lifecycle.md).
