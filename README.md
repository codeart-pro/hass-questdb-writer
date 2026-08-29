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

## Commands

```text
make validate
make up
make status
make logs
make down
```

The runtime integration is `custom_components/hass_questdb_writer`. It is a
clean implementation and does not use QSS as its code base.
