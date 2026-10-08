# Aster local application

The local application observes Lark mail and work IM, evaluates Signals, pursues YAML-defined Goals through pi, and executes auto Signals in DoubaoWork. It manages agentmemory and persists public Contexts through Local durable storage. The independent Goal web client uses streaming RPC over HTTP.

## Application composition

`src/cli.ts` parses commands. `src/services.ts` selects `LarkIntegration.layer`, `MailIntegration.layer` from `@aster/integrations` and concrete Layers from `@aster/infra` for `AsterRuntime.layer`. `src/application.ts` installs the configuration provider, owns process signals and the store lock, and runs the HTTP/runtime graph in a Scope. Cancellation covers Layer acquisition as well as the running application.

`AsterRuntime` in core owns domain service construction, root Actors, Context reactions, integration activation, readiness and shutdown. Lark owns IM readiness; memory owns its capture consumer. Local does not inspect IM state, bind ActorRefs, select Signals or initialize Goals. The host mounts `@aster/api/server` with HTTP/NDJSON; that package injects Runtime/domain services, constructs typed Actor commands and adapts replies. The API remains available while integrations catch up. During shutdown HTTP closes first, then runtime stops sources, subscriptions and Actors, drains captures and releases infrastructure. The root store lock remains held until cleanup completes; a defective shutdown retains it until process exit. Conversation storage uses the same resolved root, including paths relative to the configuration file.

External Layers are `ConfiguredDurableInfrastructure.layer`, `FileGoalScreening.layer`, `Models.configured`, the host-configured `AgentConversations.layer(root)`, `SystemOneClientLive.layer` and `AgentMemoryBackend.layer`. Runtime builds `ContextRegistry`, internal reasoning, Tasks and Goal services. Tests replace these capability Layers with isolated implementations. See [runtime design](../../docs/runtime-design.md) for the graph and contracts.

## Configuration sources

`LocalConfig.layer({ configPath, envPath, projectRoot, overrides? })` captures configuration without mutating `process.env`. Precedence is explicit structured overrides > `ASTER_*` process environment > `.env` application values > YAML > module defaults. Invalid values fail instead of falling back. Missing `.env` is allowed. Each module reads its own `Config.schema` declaration during Layer acquisition; there is no global parsed configuration service.

For example, `ASTER_HTTP_PORT=4318` changes the HTTP port; `ASTER_CONTEXTS_LARK_CHILDREN_IM_CONFIG_POLL_INTERVAL_MS=60000` changes IM polling. YAML/overrides declare dynamic keys such as Signal/Goal slugs; environment variables can override their fields (`ASTER_SIGNALS_MY_SIGNAL_TASK` for `my-signal`) but cannot introduce new dynamic keys. Model array elements use numeric indices (`ASTER_CONFIG_MODELS_0_MODEL`). Avoid names that normalize to the same environment key. Plain `.env` values are parsed once with Node's dotenv parser; interpolation is limited to credential references below.

Credential fields accept exact `${ENV_VAR}` references from captured process environment or `.env`, and are retained as Effect `Redacted` until the adapter needs them. Memory `apiKeyEnv` uses the same resolver. Arbitrary prompts remain literal. Relative memory paths resolve beside the config file. Child-process environments are explicitly supplied and service credentials/`ASTER_*` variables are filtered out of external executors.

## Goal conversations, Tasks and Signals

Configured Goals and the built-in `/goals/personal` use the same GoalActor. The main Agent handles simple dialogue and lightweight reads; sustained work uses a persistent Task with internal or external execution. Further instructions steer active work or reactivate a completed Task. Distinct work can create another Task.

All Goal and Task messages live in Pi under `<config.durable.root>/conversations`. Actor state retains business metadata, references and receipts. `GetGoalTimeline` projects actual user messages and selected assistant replies; tool records and raw Context evidence stay out of chat. There is no independent history.jsonl store. Native compaction retains older history while bounding model context; `config.goals.contextTokens` defaults to 200000 and `reserveTokens` to 8192, capped by the selected model's window.

Context changes pass System One and then the Goal's read-only relevance gate. User inputs and Task feedback bypass that second gate. Task and Signal tools return durable receipts. Signal triggers are Context conditions or schedules; both freeze and dispatch the same Task protocol. Schedules use `{ type: "once", at }` or `{ type: "cron", expression, timeZone }` inside a Schedule trigger.

Top-level `goals` maps slugs to `{title?, description, completionCriteria?}`. Restart refreshes definitions without erasing work. Goal End deactivates its Signals and cancels unstarted work; submitted Tasks remain responsible for their outcomes. Executor prompts are frozen before external confirmation; Task results return to the reply Goal.

Enable `contexts./lark.children./im` to observe recent messages from non-muted p2p and group chats. Each poll calls `im +messages-search` for a fixed time window, paginates its messages, then calls `im chat.user_setting batch_query` in groups of ten chat IDs. Muted chats are removed before child Actor updates and summarization. Missing or invalid notification settings fail the poll without advancing its cursor. Startup reads from midnight in `Asia/Shanghai`, or resumes today’s persisted retrieval progress with a one-minute overlap bounded by that midnight. Uninterrupted polling may finish the previous day’s tail across midnight. The cursor lives in private daily progress files, not public Context State. Fetched messages are durably journaled before the cursor advances. The default delay is 15 minutes after each completed polling round. Startup catch-up uses consecutive, serial query windows of at most one hour, committing each window before fetching the next. No full chat enumeration or per-chat history sweep is performed. Existing historical Contexts remain persisted; they are not deleted by this migration. Failed summaries retain their messages and retry locally after 30 seconds even if the chat receives no new messages. Startup actively restores today’s local pending summaries; prior-day backlogs remain untouched. Today’s startup summaries may initiate normal Goal screening.

Open **http://127.0.0.1:4317** after `pnpm start`. Local start serves the Vite frontend from `apps/web/src` and watches source changes; closing the browser does not stop the actors. Production builds serve `apps/web/dist`. To develop a replacement frontend on its own port, run `pnpm --filter @aster/web dev` alongside the backend.

Public loopback API:

- `POST /api/rpc`: typed queries, commands and SubscribeInvalidations from `@aster/api`, served as NDJSON RPC. The first subscription frame invalidates all queries; subsequent frames invalidate affected keys. Legacy REST and SSE endpoints are removed.

The server binds only loopback and checks Host/Origin. Model credentials are not exposed. The React/Vite frontend uses the public application API and imports no Actor or persistence internals. Browser tests use fake transports.

## IM rolling-summary core verification

Chat updates are batched for one second, screened by System One, and summarized through the replaceable `ChatSummarizer` service, implemented inside Lark integration and using `contexts./lark.children./im.config.summary.model`. Each message batch is partitioned by its message date in Beijing time. Both a daily summary and a rolling summary must be stored before their covered messages are removed. Group and direct conversations follow the same rules. Source screening now requires `stateChanged`; message-only updates do not invoke System One. Ingestion writes are excluded from evaluation, and polling cursors are private. Each summary commit journals both outputs before publishing them so an interrupted commit can be replayed without another model call. Existing inactive legacy Contexts are preserved; their old `through` field is removed when their Chat Actor next starts.

After `pnpm build`, run `node --use-env-proxy apps/local/scripts/verify-chat-summary.mjs` from the repository root. This uses synthetic evidence and real configured summary, System One and Goal models in isolated in-memory actors. It does not create a real Doubao task or require the managed memory process. A rejected relevance gate is reported as an automatic-chain failure; the script then separately checks Goal planning without changing production routing.

Follow the JSON event names `chat.summary.*`, `system-one.goals.*`, `goal.planning.*` and `delegation.*` by Context path in startup logs. Live tests reached Goal planning but also exposed inconsistent relevance classification in the configured Laya service; see `docs/im-summary-design.md`.

The app supplies `Models.configured` from `@aster/agent`. Lark summarization and Goal reasoning select independent configured model aliases. AgentRunner and DurableHarness own their SDK callback lifetimes; AgentConversations owns durable Goal and Task writers.

## Tasks and approvals

TaskActor owns `/tasks/<identity>`, execution handles, follow-up delivery and results. Internal Tasks use the Goal reasoning model; external Tasks select a configured executor. `InspectTask` returns typed business details and available tool records without provider metadata. `CheckTask` observes the original execution; `RetryTask` explicitly retries known failed work. Both retain request identity and revision; unknown submissions are never repeated automatically.

The local UI includes ApprovalQueue. Confirmation, permission and information requests use the ListApprovals and RespondToApproval RPCs. Responses include `{ decision: "approve" | "reject" }`, `{ text }`, or question answers keyed by question ID. Queue admission and response delivery persist before acknowledgement. An empty queue only means that no human decision is pending.

Codex uses app-server; busy follow-ups steer the current turn and completed work continues in the same thread. Pi continues its retained conversation. The Doubao adapter supports initial work and native decision responses but explicitly rejects general follow-up instructions because continuation is not implemented. Adapters own scheduling and uncertain delivery behavior; core does not simulate continuation with replacement submissions.

## Aster naming and existing data

Workspace packages use `@aster/*`; the CLI is `pnpm aster` (or the `aster` binary), and the default configuration is `aster.config.yaml`. Signal/Signals remain domain terms, including the `signals` configuration key and `/signals` Context paths.

Default storage and the ownership lock now live under `~/.aster`: `actors/`, `tasks/`, `im/`, and `memory/`. Memory `dataDir` supports `~/` paths; explicitly configured relative paths still resolve beside the config file. No automatic migration from the old storage directory is performed. Existing execution resume metadata retains its filename.

### IM daily files

The single-account IM integration stores its private progress and daily summaries under `~/.aster/im/`:

- `<YYYY-MM-DD>/progress.json`: successful retrieval intervals and `through`; uncovered intervals remain gaps, including on days with no messages.
- `<YYYY-MM-DD>/<chat-id>.md`: the daily summary, updated in place. YAML frontmatter includes the Beijing date, timezone, chat identity/mode, update time, first/last summarized message timestamps, and summarized message count.
- `<YYYY-MM-DD>/chats/<chat-id>.json`: pending messages, completed message fingerprints, daily summary checkpoint, failure/retry information, and any interrupted summary commit.

The rolling summary remains in the chat’s public `state.summary` (`text` and `references`). Daily summaries are separate from it. Historical backfill and historical pending-message processing are deferred; no automatic backlog migration occurs. On first upgrade, without a new daily progress file, retrieval starts at today’s midnight instead of trusting the legacy public cursor.

### IM summary admission

IM uses `config.system-one` to judge whether pending chat messages warrant a summary update. An explicit “no” retains the messages until new evidence arrives; that decision survives restarts. Errors retry after 30 seconds. System One has no added rate/concurrency limiter and does not consume summary Agent permits. This judgment is separate from Goal relevance screening after a summary changes.

Configure the following under `contexts./lark.children./im.config` (timing and concurrency values shown are defaults; select an existing model alias):

```yaml
pollIntervalMs: 900000
catchUpWindowMs: 3600000
summary:
  model: goal-reasoning
  agentStartIntervalMs: 10000
  agentConcurrency: 2
```

All chats share a FIFO Agent queue, with at least ten seconds between starts and at most two concurrent runs. Daily and rolling summaries each queue separately; retries go to the tail. Pending messages merge while waiting; the batch freezes when the daily run is admitted. Successful daily output survives a later rolling failure or restart, so only the unfinished stage retries. `~/.aster/im/agent-admission.json` preserves the latest start time. Restart reconstructs today's pending work and respects this interval.

While continuously running, successful retrieval of yesterday's final interval triggers its pending day-end summaries, including batches deferred by System One. They still use the shared Agent queue. Failed retrieval delays the flush. Restart does not revive missed prior-day work.

See [Reactive application API](../../docs/reactive-api-design.md) for the scoped HTTP/RPC implementation and browser query ownership.

The HTTP host composes `@aster/api/server`, `static-assets`, `source-assets` and `http-policy`. These are implementation modules, not new externally supplied Layers. Host/Origin and body-size checks wrap every route through the same policy.

## Local storage

Set `config.durable.root` to choose the directory covered by the process lock (default `~/.aster`). Context files, Agent conversations and screening logs live beneath this root. Relative paths resolve beside the configuration file. Context persistence uses the Local file backend; Pi conversations are owned by the agent package. External execution uses the Codex or Doubao adapters.
