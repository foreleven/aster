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

Every `ContextCommand.Class` declares its own `success` Schema and optional `error` Schema. `Schema.Void` supports an empty acknowledgement; omitting `error` means `Schema.Never`. There is no shared result envelope or default domain error. Typed Actor asks return the business value and original declared error. Dynamic Context queries erase response types; the RPC boundary encodes each command's chosen codecs and represents `void` as JSON `null`.

Commands may override `static text(result)` with a synchronous, typed formatter. Agent queries invoke it after success validation and retain the complete formatted text before replying. The default formatter encodes the success Schema as JSON. Tool content presents the text directly, with a result ID and cursor for further pages; replay and later pages reuse the retained text even after the owner stops. Direct asks and RPC queries do not invoke the text formatter.

`ContextQueryError` describes framework validation, routing and lifecycle failures. The route Scope reports owner retirement separately from the domain reply protocol. Local replies only include a framework error when the command explicitly accepts it; other local callers retain the Actor runtime's normal timeout/cancellation behavior. Defects and interruption remain outside ordinary failure replies.

Public commands enter the owning Actor's `receive`, which dispatches with `Match.tag`. Account and mailbox profiles read owner state. Goals, Tasks, Signals and IM lists enumerate current children; reads ask the corresponding child to select its own public business evidence. Task details capture mailbox state before reading selected Pi entries in a request-owned worker. Query handlers never expose raw execution transcripts or internal receipts. Local child reads remain outside the public capability catalogue.

Provider-backed queries use services inside the owning Actor. IM `messages` reads provider history independently of summary retention. Lark Mail lists provider metadata; retained email reads restore a passivated Message Actor and ask it for its state, while unretained emails use the provider. Generic Mail retains its provider day-query contract. Apps replies directly without persisting query results. Queries do not mutate synchronization state. Collection commands belong to each domain. Local lists return all matching live children and accept no generic offset/limit; provider commands retain their native cursor/page-size arguments.

## Capability catalogue

| Entry point    | Commands                            | Evidence boundary                                                                                              |
| -------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `/mail/<id>`   | `list`, `read`                      | Zoned day metadata and selected email content; see [Mail design](mail-design.md).                              |
| `/apps/<name>` | Existing application commands       | Typed provider queries; results are not persisted in AppState.                                                 |
| `/lark`        | `profile`                           | Allowlisted account identity.                                                                                  |
| `/lark/im`     | `list_chats`, `summary`, `messages` | Active Chat metadata and actor-owned summaries; messages use provider history with explicit cursor pagination. |
| `/lark/mail`   | `profile`, `list`, `read`           | Actor-owned mailbox identity, filtered provider metadata pages and selected retained/provider email.           |
| `/goals`       | `list`, `read`                      | Identity, title, description, status, summary and selected task references.                                    |
| `/tasks`       | `list`, `read`                      | Status, owner, agent, instructions and outcome; no tool transcript or executor checkpoint.                     |
| `/signals`     | `list`, `read`                      | Ownership, trigger, status, schedule cursor and selected task/version.                                         |

Domain list commands default to 20 items with a maximum of 100; Context result paging bounds large evidence returned to Agents. Domain modules own field selection; Runtime only registers their capabilities. Missing handlers have no fallback `state` command.

Memory retains `memory_search` and `memory_expand`. Goal-local tools such as `goal_current`, `task_list` and `signal_list` keep their existing roles. No compatibility aliases or secondary query stores are introduced.

## Lark query arguments

Query protocols live in `lark/account/queries.ts`, `lark/im/queries.ts` and `lark/mail/queries.ts`.

- IM `summary` takes `chatId`. `messages` accepts exactly one of `chatId` or DM `userId`, optional `start`/`end` (dates or ISO timestamps with timezone), `order` (`asc`/`desc`), `pageSize` (1–50) and `pageToken`. It maps to `im +chat-messages-list`; there is no local message offset or keyword filter on that command.
- Lark Mail `list` maps to `mail +triage`. `query` performs provider full-text search (50 characters maximum). `from`, `to`, `cc` and `bcc` are comma-separated address lists. Exact filters include `folder`/`folderId`, `label`/`labelId`, `subject`, `isUnread`, `hasAttachment` and `start`/`end` (whole-second ISO timestamps with timezone). `limit` is 1–400; `pageToken` resumes the same filter/query chain. With no folder or label, the default is inbox. `read` takes `messageId`.
- Provider pages return `items`, `hasMore` and `nextPageToken`. `coverage.complete` is false while more pages remain; it does not claim tenant-wide or inaccessible history. Querying history does not backfill Chat state or alter polling coverage.
- Domain Actor lists describe current children, including child state restored at startup. They do not include persisted records whose Actors have stopped. IM restores all retained Chat identities, even after their summary has consumed the pending messages.
