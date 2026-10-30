# Helper scripts

Use the Makefile targets to run these scripts.

| Script | Purpose |
| --- | --- |
| `create_cluster_id.sh` | Create a KRaft cluster ID for the Docker Kafka stack. |
| `update_run.sh` | Set the cluster ID before starting a broker. |
| `wait_for_kafka_health.sh` | Wait for Docker broker health checks to pass. |
| `ensure_kafka_topic.py` | Create the configured topic if it does not exist. |
| `check_docs_links.py` | Check local links in the built documentation and slides. |

For local Docker tests:

```bash
make start-docker
make test-smoke
make stop-docker
```

For documentation checks, build the full site first:

```bash
make docs
make docs-check
```

Docker services are for local tests. Production uses the site configuration in
`custom.toml`. See [Deployment](../docs/deployment.md) for setup and updates.
