# fiber

> **Moved.** fiber is now `flow` in [urmzd/orchagent](https://github.com/urmzd/orchagent). This repository is archived. Its last release only marks the Go module deprecated.

Agents on a bus: the event union, channels, connectors, the durable turn. It lived in the Humagen repository as `fiber/` before moving to orchagent.

## Migrating

```sh
go get github.com/urmzd/orchagent@latest
```

Then rewrite imports:

| Before | After |
|--------|-------|
| `github.com/urmzd/fiber` | `github.com/urmzd/orchagent/flow` |
| `github.com/urmzd/fiber/pkg/<pkg>` | `github.com/urmzd/orchagent/flow/<pkg>` |
| `github.com/urmzd/fiber/pkg/durable` | `github.com/urmzd/orchagent/flow/runschema` |

Package names follow the directory: `fiber.X` becomes `flow.X`. Names stored in databases or on the wire (`fiber_*` tables, `fiber://` URIs, `fiber.*` event kinds) are unchanged.

orchagent carries the later history from `humagen/fiber`; this repository keeps the history before the fold.
