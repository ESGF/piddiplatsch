# TODO

## Architecture cleanup

Do this in a separate PR before extending the processor system:

- Define one clear processor interface, including the preflight check.
- Decide between simple static registration and a real plugin system.
- Remove unused plugin abstractions after that decision.
- Do not hide processor import errors in the registry.
- Reduce global state for configuration and statistics.
- Pass configuration, statistics, lookups, and output paths explicitly where useful.
- Add type checking after the interfaces are settled.
- Add an architecture document describing the message-processing flow.

Keep the refactoring incremental and preserve existing behavior with tests.
