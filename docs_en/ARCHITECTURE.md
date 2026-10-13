# Architecture and Responsibility Boundaries

[中文](../docs/ARCHITECTURE.md) | [English overview](../README_en.md)

## A Persistent Messaging Bridge Around an Upstream Runtime

In the private system, each persona has its own Router, database, app-server connection, Codex home, and platform bindings. The database maintains which thread is the canonical `main`; display titles do not determine identity. If the intended session is unavailable, a platform adapter must not independently create a replacement session.

Router handles authenticated messaging-bridge input. Native desktop/mobile entry points use the upstream app-server capabilities directly. A custom external-turn observer converts known GUI activity into alias leases, causing messaging-bridge workers to defer acquiring new tasks while that activity is observed.

| Layer | Source of capability | What this repository includes |
| --- | --- | --- |
| Model inference, tool execution, native thread protocol, sandboxing | Upstream Codex and model services | No implementation; tests use a fake client |
| Telegram/Feishu transport and authentication | Platform APIs/SDKs and custom adapters | Normalization only; no online transport |
| Owner/endpoint bindings, ordering, deduplication, leases, outbox | Custom Router | Implementation and tests |
| SQLite transactions, WAL, and persistence primitives | SQLite | Calls to the system library through cgo; no bundled SQLite source |
| Main identity, generation transitions, and epoch ledger | Custom control/storage mechanisms built on Codex threads | Storage transactions and history; no owner-operation programs |
| Permission profiles, release bindings, startup and runtime preflight checks | Custom policy/release integration, plus OS and upstream execution constraints | Architecture description only |
| Long-term memory selection and indexing | MQElite | Separate showcase repository |

The guardian protecting main and the owner-only rotation tools belong to the private system but are not part of this code selection. They separate ordinary messages from authority to replace the main session. Rotation requires quiescent state and commits the alias, generation, and epoch history in one transaction. Because app-server provides no atomic remote input gate, a brief remote race window remains, requiring the owner to coordinate input during the operation. A database transaction therefore does not atomically lock all GUI input.

## Code Layout

| Package/file | Responsibility |
| --- | --- |
| [`adapters`](../internal/adapters/adapters.go) | Normalize authenticated events and enforce input-size limits |
| [`model`](../internal/model/model.go) | Task states, typed failure boundaries, and shared records |
| [`store`](../internal/store/store.go) | SQLite transactions, deduplication, queues/outbox, leases, and lifecycle records |
| [`store/main_history.go`](../internal/store/main_history.go) | Committed epochs, migration coverage boundaries, and integrity verification |
| [`router`](../internal/router/router.go) | Input acceptance, workers, submission boundaries, and delivery |
| [`router/external.go`](../internal/router/external.go) | Convert known external turns into alias leases |
| [`appserver`](../internal/appserver/correlation.go) | Match notifications by both thread ID and turn ID |
| [`activity`](../internal/activity/activity.go) | Bounded lifecycles for typing/reaction indicators |
| [`mainhistory`](../internal/mainhistory/server.go) | Bounded, read-only history protocol over a private Unix socket |
| [`securefs`](../internal/securefs/securefs.go) | Checks for private permission modes, symbolic links, and atomic replacement |

Dependencies among these packages are all within the repository. The original module name is retained to avoid changing import paths; it does not imply that this is a published Go module address.

## Applicability and Limits

The code targets Unix/macOS and was tested only on macOS for this snapshot. The original cgo linker directives target Darwin, so this extraction should not be treated as a verified portable Linux package. Owners, threads, endpoints, and keys used in tests are synthetic. The repository contains no private configuration, real messages, account bindings, session transcripts, credentials, deployment binaries, or online databases.

Direct filesystem checks strengthen the application's own state handling; they do not create a new OS sandbox or protect against malicious processes under the same login user that can access the state and keys. The HMAC chain detects inconsistencies under its key assumptions; it is not an audit log witnessed by an independent third party.
