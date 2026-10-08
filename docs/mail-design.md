# Mail Context design

Status: implemented.

## Retrieval and evidence

Initial automatic retrieval starts at today's midnight in the mailbox's configured IANA time zone, defaulting to `Asia/Shanghai`. The first retrieval admits existing emails from today individually. Each email becomes a durable Context and produces source evidence for Goal and Signal matching. Replaying an unchanged provider identity does not emit another event. Goal screening decides relevance; admission does not imply a user notification.

The mailbox index contains today's summaries only. Rollover clears that index independently of remote retrieval. It never deletes individual Email Contexts, which remain available to existing Goals and Tasks after the remote message disappears.

`through` is a durable ISO timestamp with an explicit offset. A poll fixes its upper bound before fetching, retrieves the entire interval, durably publishes each admitted email, then advances coverage and the discovery checkpoint together. Empty successful intervals advance coverage too. Failure preserves the previous cursor. Recovery resumes from the saved boundary day by day, including across multiple offline days. Recovered older emails enter matching without repopulating today's index.

`startedAt` fixes the initial day across failed retrieval and restart. Before fetching bodies, the mailbox persists an initial provider identity baseline. Initial old emails remain history-only. Later unknown identities are admitted even if their dates precede `through`; original dates determine their index membership. The private `known` identity checkpoint advances only after publication. Identity rediscovery alone never suppresses an interrupted publication retry.

## State and ownership

| File                     | Responsibility                                                                |
| ------------------------ | ----------------------------------------------------------------------------- |
| `mail/model.ts`          | Configuration, normalized transport messages, retrieval windows and batches.  |
| `mail/state/snapshot.ts` | Serializable `MailboxSnapshot` Schema.                                        |
| `mail/state/model.ts`    | Actor-local `MailboxState` Effect service and business transitions.           |
| `mail/actors.ts`         | Polling, lifecycle, result generations, rollover and child email publication. |
| `mail/client.ts`         | Scoped IMAP/POP3 SDK boundaries and provider completeness checks.             |
| `mail/queries.ts`        | Read-only mailbox command definitions and handlers.                           |
| `mail/dates.ts`          | Calendar validation, zoned day boundaries and interval membership.            |

`MailboxState` uses `ContextRegistry` and the existing `DurableContext` store. It introduces no independent Store or Ref. Canonical persistence, committed memory, revision checks, source journals and subscriptions remain owned by Context infrastructure. Every business transition runs inside the mailbox Actor and commits against the revision it read.

The model exposes `snapshot`, `baseline`, `nextWindow`, `rollDay`, `completeSync` and `failSync`. It has no generic field setter and does not own remote clients, child Actors or timers. Mailbox commits update readers without emitting source events; individual Email Contexts produce those events.

The public snapshot exposes `timeZone`, `dateBasis`, `through`, `today`, `status`, `undatedObserved` and sanitized `lastFailure`. The initial boundary and provider identity inventory remain private. Polling and queries share one provider permit per mailbox, avoiding conflicting POP3 sessions. Both belong to the Actor scope; stopping it cancels remote work and removes its command catalogue.

## Provider contracts

- IMAP identities include folder, UIDVALIDITY and UID. Day membership uses server `INTERNALDATE`. Date searches produce widened candidates; precise half-open timestamp filtering happens before downloading bodies. Every requested header must be returned before a scan can succeed. Missing headers or disappearing bodies fail the interval rather than advancing coverage.
- POP3 identities use UIDL, independent of sequence renumbering. POP3 has no server receipt date; day membership uses the sender's `Date` header. `TOP` retrieves headers without downloading historical bodies; UIDL and TOP support are required.
- Polling scans the provider identity inventory to detect late arrivals. No fixed latest-message cap or finite date overlap substitutes for discovery. Inventory cost grows with the remote mailbox; POP3 additionally scans headers because it cannot search dates.
- Missing or invalid dates are never replaced with now. Responses report `undatedObserved`, the number encountered by that scan. IMAP date searches may exclude unassignable records, so this is not a global undated-mail count.
- An IMAP UIDVALIDITY change fails synchronization and retains progress. Automatic identity reconciliation is not implemented: treating all replacement UIDs as new would incorrectly admit historical mail. Retained local evidence remains readable.
- No provider strategy can recover mail deleted before it was observed. Query pagination is a live provider view rather than a frozen snapshot.

## Read-only commands

Mailbox capability discovery follows the shared [Agent Context access contract](context-agent-access-design.md).

- `list({ date?, query?, offset?, limit? })`: date is `YYYY-MM-DD`, default today. Query matches sender and subject without case sensitivity. Results contain metadata without bodies, newest first, calendar zone/date basis, preceding date, pagination and coverage. Default page size is 20, maximum 100. `coverage.complete` covers dated matches in the successfully retrieved interval; `undatedObserved` separately reports unassignable observations.
- `read({ id })`: reads retained individual evidence when present, otherwise fetches the mailbox-scoped provider identity. Returns metadata and text with the date basis.

Neither command advances the cursor or discovery checkpoint, changes the default index, creates an Email Context, or triggers matching. Task Agent query evidence is retained in its Pi conversation by the existing query-result mechanism; no extra historical mailbox cache is added. Direct application RPC calls do not imply conversation persistence. No in-memory table dependency is needed.

## Runtime and validation

Each mailbox retrieves independently. Runtime readiness waits for a successful current-activation catch-up and durable publication for every configured mailbox. A saved ready flag cannot satisfy a new activation. Transport failures retry at the configured polling interval with sanitized diagnostics. Actor shutdown cancels retrieval, queries and timers; admitted durable commits retain the Context store's drain semantics.

Local tests cover provider identity stability, incomplete IMAP scans, cancellation, strict calendar dates and DST, publication-before-cursor ordering, restart baselines, multi-day recovery, rollover during an in-flight request, query isolation and retained evidence reuse. Tests use fake transports and TestClock.
