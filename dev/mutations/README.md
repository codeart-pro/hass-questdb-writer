# Mutation checkers

Each script copies nothing and changes nothing permanently: it edits one file of
the package inside the container's test tree, runs a focused test selection, and
restores the file. A mutation that leaves the tests green means the test does not
protect the behaviour it claims to protect.

They exist because a test that cannot fail is not evidence, and because the ADRs
point at them when they claim "N mutations of this fix turn the new tests red".

## Running one

```bash
C=$(podman ps --format '{{.Names}}' | grep homeassistant | head -1)
podman exec $C sh -c 'rm -rf /tmp/run && mkdir -p /tmp/run'
podman cp custom_components $C:/tmp/run/
podman cp tests $C:/tmp/run/
podman cp dev/mutations/mutation_check_reclaim.py $C:/tmp/
podman exec -w /tmp/run -e PYTHONPATH=/tmp/run $C python3 /tmp/mutation_check_reclaim.py
```

The scripts read and write `/tmp/run/custom_components/...` (the tree the tests
run against), so refresh that tree before every run.

| script | covers |
|---|---|
| `mutation_check_config_flow.py` | the config-flow identity rules (reauth keeps credentials only, reconfigure moves the `unique_id`) |
| `mutation_check_connections.py` | one connection resolver for every consumer (worker, sensor, diagnostics, config flow) |
| `mutation_check_reclaim.py` | the storage-pressure work: incremental auto-vacuum, drained vacuum, WAL truncation, reclaim-before-pause, pause counting, the shutdown wait, delivery-side recovery, startup classification, the reserve boundary |
