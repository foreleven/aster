# Business integrations

`@aster/integrations` connects Aster to external business systems. It contains `lark/` for Lark accounts, IM, and mail, and `mail/` for generic mail retrieval. See [Lark](src/lark/README.md) for its Actor, readiness, publication, and recovery contracts.

Integrations depend on core contracts and the existing Actor/Agent packages. They receive model and decision services through Effect dependencies, never by importing `@aster/infra`. The local host selects integration Layers; `AsterRuntime` owns their activation, readiness, and shutdown. This package does not re-export infrastructure, memory backends, or Models.

The generic mail adapter is exported from `mail/`. `MailIntegration.services` provides the configured `MailSettings` and `MailFetcher` services; `MailSettings` reads `contexts./mail.config.mailboxes` from the captured `ConfigProvider`, while `mailFetcherLayer()` is available for standalone composition. Passwords are decoded into `Redacted` values. Each mailbox may set `protocol: "imap" | "pop3"`, `host`, `port`, `secure`, `username`, `password`, `folder`, and `timeZone`; IMAP and POP3 are supported because SMTP is a submission protocol.

Transport and protocol tests live here. Cross-integration, source-to-Memory, and application lifecycle tests remain in `apps/local/test`. Tests use fake transports.

## Generic mail Contexts

A configured `/mail` now installs a runtime-managed source. The root lists its mailbox paths; `/mail/<mailbox-id>` exposes the calendar zone, date basis, today’s summaries, durable coverage cursor, sync status, and a sanitized failure. Both levels are committed before the first remote request. Each mailbox polls independently, with a default `pollIntervalMs` of 30,000. An absent `/mail.config` disables the source; an explicitly configured empty mailbox list, duplicate IDs, or non-positive intervals or invalid time zones fail configuration validation.

Each retrieved message becomes `/mail/<mailbox-id>/<message-id>`, with path segments percent-encoded. IMAP identities include folder, UIDVALIDITY, and UID; POP3 identities use UIDL and require server support. Message IDs do not depend on mutable sequence numbers or an optional RFC Message-ID header. Persisted messages deduplicate repeated retrieval and restart replay. Credentials and transport configuration are never copied into public Contexts.

Initial polling starts at today's midnight (`Asia/Shanghai` by default). Recovery resumes from the saved timezone-qualified `through` cursor day by day. A private provider identity baseline distinguishes initial historical mail from later backdated arrivals. Every admitted email is durably published before progress advances. Midnight clears only the daily summary index; individual Email Contexts remain retained. Mailbox `list` and `read` commands query history on demand without changing synchronization progress or triggering source matching. IMAP filters by INTERNALDATE; POP3 uses the sender's Date header and requires UIDL and TOP. Incomplete scans fail without advancing coverage. See [Mail design](../../docs/mail-design.md) for the state model and protocol limits.

Runtime readiness waits for each configured mailbox's first successful retrieval and durable publication in the current activation. Empty successful snapshots count as ready; transport failure remains visible and retries without blocking other mailboxes. Mailbox Actors own polling Fibers; shutdown interrupts retrieval and timers before storage is released. Child email Actors acknowledge only after their durable commit, so an interrupted publication is safely deduplicated on replay.

`MailIntegration.services` provides settings and the real transport for standalone callers. `MailIntegration.installation` accepts injected `MailSettings` and `MailFetcher` for lifecycle tests or alternate transports. `MailIntegration.layer` composes both; it registers a source and does not itself open mailbox connections.

Mailbox failures expose `lastFailure` with a sanitized stage, reason, allowlisted error code, and IMAP response status. Logs use `mail.poll.started`, `mail.poll.failed` (warning, with attempt and retry delay), `mail.poll.recovered` (info after durable publication), and `mail.poll.completed` (info for initial sync, debug for subsequent successful polls). Success clears the previous failure fields. Raw SDK errors, protocol commands/responses, usernames, passwords, and message contents are not logged.

## On-demand application Contexts

`AppsIntegration` registers `/apps` and the configured `/apps/xiaohongshu` and `/apps/ctrip` children. Configuration uses direct child keys under `contexts./apps`, as shown in `aster.config.example.yaml`; descriptions are optional and have defaults. Omit a child to disable it. Unsupported children and invalid descriptions fail configuration validation. The root lists enabled paths, and each child publishes `mode: query-only`. Its JSON Schema command catalogue is available through `describe_context`, independently of query execution. Startup commits those Contexts and binds their query handlers without starting OpenCLI or opening browser tabs. There is no polling, automatic retry, or source-event production.

Queries are available to Goals and Personal through `query_context`, and to application clients through the `QueryContext` RPC (`path`, `command`, `args`). For example:

```json
{
  "path": "/apps/ctrip",
  "command": "hotel-search",
  "args": { "city": "43", "checkin": "2027-02-05", "checkout": "2027-02-10", "limit": 5 }
}
```

The supported commands are:

- Xiaohongshu: `search`, `note`, `comments`, `user`, `feed`. Search supports `sort`, `noteType` and `publishTime`. Note/comment queries require the complete signed URL returned by search, including `xsec_token`.
- Ctrip: `search`, `hotel-suggest`, `hotel-search`, `hotel`, `attraction`, `flight`, `flight-round`, `train`, `bus`, `ferry`, `cruise`, `tour`, `package`. Hotel and attraction searches use numeric city IDs; flights use uppercase IATA codes; date arguments use valid `YYYY-MM-DD` calendar dates. Check-out must follow check-in, and return cannot precede departure.

Commands use named scalar arguments, validated against the published catalogue. Lists default to 10 results, with a maximum of 50 (30 for hotel searches). Login, posting, following, downloads, booking and payment are not exposed. Each successful response includes the Context path, command, query timestamp and JSON data; queries reply directly without persisting results in the Context snapshot. Agent tools retain paginated results within an evaluation using `read_query_result` without repeating the browser query. Results and page content remain untrusted evidence.

Install OpenCLI separately and make `opencli` available on the host's PATH. Configure its browser bridge and log into the relevant websites before using browser-backed queries. Adapters were checked against OpenCLI 1.8.8 and the upstream [Ctrip](https://opencli.info/docs/adapters/browser/ctrip.html) and [Xiaohongshu](https://opencli.info/docs/adapters/browser/xiaohongshu.html) documentation. OpenCLI/site updates may require adapter changes; Aster does not perform automatic installation, login or captcha bypass. Future travel inventory may not yet be on sale, and prices are query-time observations.

The transport uses Effect's scoped child-process spawner with explicit argv, no shell, a filtered captured environment, a 90-second deadline and a 128 KiB stdout limit. Browser queries share one semaphore; each Context queues commands through a Behavior-owned CommandProcessor with concurrency one. Caller cancellation and Actor shutdown release process work, and retiring Actors unregister their query routes. Immediately before spawning, `apps.query.command` logs the full shell-quoted command, including query arguments and any signed note URL, so the user can copy it into a terminal. Environment variables are not printed. Execution still uses argv directly with `shell: false`. The other `apps.query.started`, `apps.query.completed` and `apps.query.failed` logs contain path, command or sanitized failure details; raw process output is not logged. On nonzero exit, the transport retains at most 16,384 characters of stderr while draining the pipe. OpenCLI 1.8.8 emits a YAML error envelope to stderr even with `-f json`; Aster decodes YAML/JSON diagnostics and exports only a bounded, redacted code/message/help summary. Failure logs always include the command. Browser connection, argument, timeout and session-busy codes retain typed categories; authentication and site security failures include the upstream explanation without automatic retry. Malformed diagnostics produce an explicit fallback rather than dumping browser content. `opencli doctor` checks the local daemon and extension; a healthy bridge does not prove site login or query success.

`AppsIntegration.installation` accepts injected `AppsSettings` and `OpenCli` services for tests. `AppsIntegration.layer` composes the real services and requires the host's `ChildProcessSpawner` and captured `ProcessEnvironment`. Runtime owns activation and shutdown. Tests use fake processes and real Actor/runtime wiring, with no live site queries.
