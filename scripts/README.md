# Test Scripts (Docker/Smoke)

This folder contains helper scripts used exclusively for local Docker services and smoke tests. They are not required for production deployments.

What these scripts do:
- `create_cluster_id.sh`: Generates a KRaft Cluster ID for the demo Kafka stack.
- `update_run.sh`: Injects the generated Cluster ID into broker env before startup.
- `wait_for_kafka_health.sh`: Waits for brokers to become healthy via `/dev/tcp` hostname probes.
- `ensure_kafka_topic.py`: Ensures the configured topic exists before starting the consumer or running smoke tests.

How to use (via Makefile):
- Start services: `make start-docker`
- Run smoke tests: `make test-smoke`
- Stop services: `make stop-docker`

Notes:
- These scripts are wired through `docker-compose.yml` and the `Makefile` targets above.
- For non-Docker ESGF Kafka setups, use `etc/esgf-example.toml`. Plain
  `consume` writes raw and Handle JSONL without publishing; add `--publish`
  only when Handle-service writes are intentional.
