# Agent Context access

Status: implemented.

## Boundary

Task Agents discover capabilities, inspect command schemas, then request domain evidence. They cannot dump arbitrary persisted Context state. Primary Goal Agents retain their small coordination tool set and delegate evidence retrieval to Tasks.

Web's `ListContexts` and `GetContext` RPCs retain their application projections. This change does not redesign Web data loading. The existing application `QueryContext` route remains available and now uses the same command validation as Agent queries.

## Tools

| Tool                | Contract                                                                                                                                  |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `list_contexts`     | List active capability paths and descriptions, 20 per page, optionally restricted to descendants of `parent`. No state or message bodies. |
| `describe_context`  | Read an entry point's command names, descriptions and JSON argument schemas. No previous results.                                         |
| `query_context`     | Execute a supported read-only command with strict argument validation. Retain Agent evidence in the bound Pi conversation.                |
| `read_query_result` | Page previously retained evidence without repeating a provider query.                                                                     |

`search_contexts` and `read_context` were removed. An Agent may reuse a known command schema; execution always validates arguments. Individual stored emails and Tasks are not capability entry points when their owning collection already provides list/read commands. Task tool catalogue version is `aster.task.v3`.

## Registration and execution

`ContextQueries.register(path, { description, commands }, handler)` registers metadata and its handler in the owner's Scope. Each command supplies a description and Effect Schema. That Schema drives both JSON Schema publication and strict runtime validation, including rejection of unsupported commands and excess fields. The capability registry is independent of persisted business state.

Owner release removes capabilities and cancels active queries. Waiting callers receive an unavailable failure when that owner retires. Caller cancellation, defects and unrelated interruption retain their semantics. The Contexts Actor bounds Agent query execution to four requests; Pi retains results before tool acknowledgement. Integrations can impose stricter provider concurrency limits.

Public commands describe read-only business operations, not internal Actor protocols. Apps adapts them to typed Actor asks because successful queries persist its last result. Read-only domain handlers select explicit business fields from canonical snapshots and Pi; they do not send commands back to their owner merely to read committed state. Generic Mail history queries never mutate synchronization state.

## Capability catalogue

| Entry point    | Commands                            | Evidence boundary                                                                            |
| -------------- | ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `/mail/<id>`   | `list`, `read`                      | Zoned day metadata and selected email content; see [Mail design](mail-design.md).            |
| `/apps/<name>` | Existing application commands       | Schemas move out of persisted AppState; query persistence is unchanged.                      |
| `/lark`        | `profile`                           | Allowlisted account identity.                                                                |
| `/lark/im`     | `list_chats`, `summary`, `messages` | Chat metadata and retained evidence; messages explicitly report partial historical coverage. |
| `/lark/mail`   | `profile`, `list`, `read`           | Mailbox identity, provider day results and selected retained/provider email.                 |
| `/goals`       | `list`, `read`                      | Identity, title, description, status, summary and selected task references.                  |
| `/tasks`       | `list`, `read`                      | Status, owner, agent, instructions and outcome; no tool transcript or executor checkpoint.   |
| `/signals`     | `list`, `read`                      | Ownership, trigger, status, schedule cursor and selected task/version.                       |

Domain list commands default to 20 items with a maximum of 100; Context result paging bounds large evidence returned to Agents. Domain modules own field selection; Runtime only registers their capabilities. Missing handlers have no fallback `state` command.

Memory retains `memory_search` and `memory_expand`. Goal-local tools such as `goal_current`, `task_list` and `signal_list` keep their existing roles. No compatibility aliases or secondary query stores are introduced.
