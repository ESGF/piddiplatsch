# Piddiplatsch

Piddiplatsch consumes ESGF STAC publication records from Kafka and registers
persistent identifiers with the Handle System. Its staged workflow is
`harvest → map + validation → publish`, with JSONL files between stages.

- [Installation and quick start](https://github.com/ESGF/piddiplatsch#readme)
- [Architecture](architecture.md): routing, project plugins, and processing stages.
- [Configuration](configuration.md): site settings and supported options.
- [Operations](operations.md): output, publication, retries, and monitoring.
- [Deployment](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md):
  Linux service setup with Ansible.

Visit [Talks](talks.md) for an introduction to Piddi and its
ESGF-NG workflow. The deck includes an operational appendix and speaker notes.

See [Building the documentation](building.md) to preview or publish this site
and the slides.
