# Piddiplatsch

Piddiplatsch consumes ESGF STAC publication records from Kafka and registers
persistent identifiers with the Handle System. Its staged workflow is
`harvest → map + validation → publish`, with JSONL files between stages.

- [Installation and quick start](quick-guide.md)
- [Architecture](architecture.md): routing, project plugins, and processing stages.
- [Configuration](configuration.md): site settings and supported options.
- [Operations](operations.md): output, publication, retries, and monitoring.
- [Recovery & retry](recovery.md): advanced recovery procedures.
- [Deployment](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md):
  Linux service setup with Ansible.

Visit [Talks](talks.md) for an introduction to Piddi and its
ESGF-NG workflow. The deck includes an operational appendix and speaker notes.

See [Building the documentation](building.md) to preview or publish this site
and the slides.

## About the project

Piddiplatsch is developed for the ESGF community and used in production at
DKRZ. It supports sites managing CMIP dataset and file records, with built-in
plugins for CMIP6, CMIP6Plus, CMIP7, and CORDEX-CMIP6. Contributions from other
ESGF sites and organizations with similar workflows are welcome.

The name is inspired by the TV puppet
[Pittiplatsch](https://en.wikipedia.org/wiki/Pittiplatsch), with a phonetic PID
pun and the short CLI name `piddi`: *curious by nature, persistent by design*.
