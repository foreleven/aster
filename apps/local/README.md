# Aster local application

The local application observes Lark mail and work IM, evaluates Signals, pursues YAML-defined Goals through pi, and executes auto Signals in DoubaoWork. It manages agentmemory and persists public Contexts in local JSON/JSONL files. The independent Goal web client uses HTTP and SSE.

## Application composition

`src/cli.ts` parses commands. `src/services.ts` selects `MemoryIntegration.layer`, `LarkIntegration.layer` and concrete infrastructure Layers for `AsterRuntime.layer`. `src/application.ts` installs the configuration provider, owns process signals and the store lock, and runs the HTTP/runtime graph in a Scope. Cancellation covers Layer acquisition as well as the running application.

`AsterRuntime` in core owns domain service construction, root Actors, Context reactions, integration activation, readiness and shutdown. Lark owns IM readiness; memory owns its capture consumer. Local does not inspect IM state, bind ActorRefs, select Signals or initialize Goals. HTTP calls `runtime.api` use cases and maps their errors; it does not construct domain Commands or assemble history. The API remains available while integrations catch up. During shutdown HTTP closes first, then runtime stops sources, subscriptions and Actors, drains captures and releases infrastructure. The process lock remains held until cleanup completes.

External Layers are `FileContextStore.layer`, `FileGoalHistory.layer`, `Models.configured`, `SystemOneClientLive.layer`, `AgentMemoryBackend.layer` and `ExternalAgentsLive.layer`. Runtime builds `ContextRegistry`, internal reasoning, Task preparation and Goal services. Tests replace these capability Layers with isolated implementations. See [runtime design](../../docs/runtime-design.md) for the graph and contracts.

## Configuration sources

`LocalConfig.layer({ configPath, envPath, projectRoot, overrides? })` captures configuration without mutating `process.env`. Precedence is explicit structured overrides > `ASTER_*` process environment > `.env` application values > YAML > module defaults. Invalid values fail instead of falling back. Missing `.env` is allowed. Each module reads its own `Config.schema` declaration during Layer acquisition; there is no global parsed configuration service.

For example, `ASTER_HTTP_PORT=4318` changes the HTTP port; `ASTER_CONTEXTS_LARK_CHILDREN_IM_CONFIG_POLL_INTERVAL_MS=60000` changes IM polling. YAML/overrides declare dynamic keys such as Signal/Goal slugs; environment variables can override their fields (`ASTER_SIGNALS_MY_SIGNAL_TASK` for `my-signal`) but cannot introduce new dynamic keys. Model array elements use numeric indices (`ASTER_CONFIG_MODELS_0_MODEL`). Avoid names that normalize to the same environment key. Plain `.env` values are parsed once with Node's dotenv parser; interpolation is limited to credential references below.

Credential fields accept exact `${ENV_VAR}` references from captured process environment or `.env`, and are retained as Effect `Redacted` until the adapter needs them. Memory `apiKeyEnv` uses the same resolver. Arbitrary prompts remain literal. Relative memory paths resolve beside the config file. Child-process environments are explicitly supplied and service credentials/`ASTER_*` variables are filtered out of external executors.

## Goal tasks, history and timers

Goals keep a flat `state.tasks` list and native `AgentMessage[]` working messages. The Agent has `task_list/get/create/update/delete/execute`, `signal_list/get/create/update/delete`, and paginated `goal_history` tools. Task content changes require the current revision; changing a pending proposal revokes its old confirmation. Task completion and execution completion are separate. Deleted tasks retain their history; running delegations finish, pending ones cannot start. Signals are deleted separately.

Goal-owned Signals notify the Goal to assess evidence. A round may record only a milestone/conclusion, maintain tasks or monitoring, or propose an execution. Goal executions always enter user confirmation; confirming a task does not approve separate external writes. Independent Signals continue to use their configured execution mode.

Full history is stored at `~/.aster/goals/{slug}/history.jsonl`. Context `messages` contain only the working window, alongside `state.summary`; the web inspector's AtomRpc feed reads full history through `GetGoalHistory`; legacy clients can use `GET /api/goals/{slug}/history?before=<sequence>&limit=30`. The Tasks tab shows active tasks and execution links. `config.goals.contextTokens` defaults to 48000 and `reserveTokens` to 8192. Budget checks use a conservative UTF-8 byte upper bound, include tool declarations, and apply before every provider request. Older complete exchanges are summarized; full history is retained. Failed compaction preserves the previous summary/boundary and prevents an oversized invocation. Very large exchanges are summarized in bounded evidence pages. Interrupted tool calls retain their original calls plus an explicit unknown-result record, so the next round must inspect current state before retrying.

Signal `schedule` is a serializable object, converted to Effect scheduling at runtime:

```yaml
schedule:
  type: once
  at: "2026-10-01T20:00:00+08:00"
# Alternatively:
# schedule:
#   type: cron
#   expression: "0 20 * * *"
#   timeZone: Asia/Shanghai
```

Scheduled Signals are assessed on timer occurrences, rather than on every source update. `when` describes what should be assessed; Goal timers wake the Goal to read current evidence. Optional `notBefore` is an absolute timestamp gate. Persisted deadlines survive restart; missed occurrences coalesce into one wake-up, and completed one-time schedules stay completed. Updating/deleting a Signal invalidates old timer messages. Timer delivery never bypasses execution confirmation.

Top-level `agents.doubao.prompt` configures default executor instructions. Defaults allow read-only investigation and new reports/drafts in the dedicated task workspace; existing-file changes and external writes require individual confirmation. The prompt is applied during preparation and Doubao submission, together with the existing `AskOnRisk` interaction bridge. Prepared tasks and confirmation prompts use readable sections rather than compact JSON. Preparation searches memory, expands available candidates, and supplies current Goal/prior execution evidence. Trigger and terminal-outcome memory captures have separate identities; failed captures are persisted and retried every 30 seconds with at most two in flight.

This revision expects fresh local Goal data; existing state is not migrated or automatically deleted. The user handles cleanup before validation. No application startup or live external delegation is required by the automated tests.

## Start

1. Copy `aster.config.example.yaml` in the repository root to `aster.config.yaml`. Set `/lark.config.profile` to a profile from `lark-cli profile list`.
2. Set `config.system-one` (`url`, `model`, `apiKey`). The example references `TYPESAFE_API_KEY` in the repository root `.env`. Ensure the selected Lark profile can read your account and mailbox. Codex CLI must be installed and authenticated for dynamic descriptions and Signal extraction (verified with `0.156.1`, including named permission profiles).
3. Run `pnpm install`, `pnpm build`, then `pnpm start`. Stop with Ctrl+C.

The local config, `.env`, and project-local `.aster/` and legacy `.signals/` are ignored by Git. A different config can be selected with `pnpm aster start --config path/to/config.yaml`. Relative memory `dataDir` paths resolve beside the config file.

If the System One service needs a proxy, use `HTTPS_PROXY=http://127.0.0.1:7897 HTTP_PROXY=http://127.0.0.1:7897 pnpm start`. Local memory connections bypass the proxy.

The mail Actor uses its first successful poll as a baseline; historical mail is not ingested. Later polls create `/lark/mail/me/{message_id}` Contexts. Each Context exposes only `path`, fixed `description`, object `state`, and ordered `messages`. Actor services stay private; persisted ingestion positions and execution bookkeeping are currently included in structured state. Public snapshots are persisted under `~/.aster/actors/{path}/state.json` and `messages.jsonl`. Each JSONL line is a domain Message; mailbox Commands are not stored. The store recovers an interrupted two-file commit before loading snapshots.

Root Actors are registered in code, even when their YAML entries are absent. The loader passes root subtrees unchanged; Lark validates its own `children./mail` and optional `children./im` configuration and starts its children. Omitting Lark config uses the CLI's selected profile, the `me` mailbox, and a 30-second poll interval.

Account and mailbox Actors independently load their identity profiles on startup. The common Context processor captures these profiles in separate memory sessions. Ordinary emails do not write activity memory. The configured System One model selects candidates; Codex confirms triggerings. An initialized `/signals/{slug}/runs/{id}` supplies frozen public snapshots of the run and source to one memory session. The session ends after observation processing completes. Auto runs submit to DoubaoWork and collect results in a Delegation Context. Confirm-mode runs appear in the unified approval queue; outcome capture uses a separate memory session.

## System One configuration

The top-level `config.system-one` selects the decision service independently of Contexts and memory models:

```yaml
config:
  system-one:
    url: https://api.typesafe.ai/v1/systemone
    model: jev-latest
    apiKey: ${TYPESAFE_API_KEY}
```

`url` accepts the service root, its `/v1` base, or the full `/v1/systemone` endpoint. `model` is sent on every decision request. `apiKey` accepts a literal string or an exact `${ENV_VAR}` reference resolved from the startup environment, including the project-root `.env`. The settings are required when Signals or Goals are configured. Credentials are not part of public Contexts or memory snapshots.

For the local Laya service:

```yaml
config:
  system-one:
    url: http://10.37.90.111:8000/v1/systemone
    model: multilingual
    apiKey: ${LAYA_API_KEY}
```

The application uses the System One protocol through the TypeSafe SDK with explicit address, model, and credential settings. It does not implicitly select the backend from TypeSafe environment variables.

## Memory configuration

Aster owns `@agentmemory/agentmemory@0.9.29` and its pinned iii engine `0.11.2`. The CLI runs without onboarding or Agent hook installation. On first startup it may download the engine to `~/.agentmemory/bin`. Aster waits for readiness and stops its own worker and engine on exit. Memories remain under `/memory.config.dataDir`.

`/memory` can be omitted to use the defaults: description `My long-term memory`, data directory `~/.aster/memory`, REST port `3111`, synthetic compression, and BM25 recall. The managed service also uses port + 1 (streams), port + 2 (viewer), and port + 46023 (engine), all bound to loopback. The pinned upstream CLI uses global PID files, so this version runs one managed memory instance per user and refuses to adopt an existing daemon.

To enable LLM compression and session summaries:

```yaml
contexts:
  /memory:
    description: My long-term memory
    config:
      dataDir: ~/.aster/memory
      port: 3111
      llm:
        provider: openai
        model: your-model
        baseUrl: https://your-service/v1
        apiKeyEnv: MEMORY_LLM_API_KEY
```

Put `MEMORY_LLM_API_KEY` in the root `.env`. Key values never enter public Contexts. Providers: `openai`, `anthropic`, `minimax`, `gemini`, `openrouter`. Custom `baseUrl` is available for the first three. OpenAI-compatible services must support chat completions; a `/v1/systemone` endpoint alone is insufficient. `autoCompress` defaults to true with an LLM and can be disabled; session summaries still use the LLM. Without an LLM, agentmemory creates short synthetic observations and skips semantic session summaries.

The example config enables MiniMax's China service for memory compression and summaries:

```yaml
llm:
  provider: minimax
  model: MiniMax-M3
  baseUrl: https://api.minimax.cn/anthropic
  apiKeyEnv: MINIMAX_API_KEY
autoCompress: true
```

Set the named variable in the root `.env`; `apiKeyEnv` may reference any existing key variable. agentmemory's MiniMax provider appends `/v1/messages` to `baseUrl` and uses the Anthropic-compatible protocol. Restart the application after editing model settings. Memory retrieval continues to use BM25 unless embeddings are explicitly enabled.

Embeddings are a separate opt-in:

```yaml
embedding:
  provider: local
```

The local model may download on first use. Remote providers are `openai`, `gemini`, `voyage`, `cohere`, and `openrouter`, with `apiKeyEnv` required. OpenAI/OpenRouter accept `model`; OpenAI accepts an embedding-specific `baseUrl`. Other embedding models are fixed upstream. The pinned package shares a provider's credential between LLM and embeddings, so same-provider configurations need the same key. Configurations that would let an embedding key override the selected LLM are rejected; local/OpenAI embeddings are compatible with any selected LLM.

## Recall

While Aster is running:

```sh
pnpm aster memory search "What is my work email address"
pnpm aster memory search "work email" --limit 5 --config path/to/config.yaml
# Replace the placeholders with obsId and sessionId from a relevant search result.
pnpm aster memory expand "<obsId>" --session "<sessionId>"
# Multiple IDs are supported; omit --session when they belong to different sessions.
pnpm aster memory expand "<obsId-1>" "<obsId-2>"
```

Search returns `{ mode: "compact", results: [...] }`, with up to 10 candidates by default: `obsId`, `sessionId`, title, type, score, timestamp, and available `sourcePaths`. It does not automatically load narratives, facts, or full observations. `--limit` accepts 1–100. Expand returns `{ mode: "expanded", results: [...], truncated: false }` containing the selected records' details. Each expansion processes at most 20 IDs; `truncated: true` means additional IDs need another call. Both commands support `--config FILE`.

The extraction Agent searches when useful, expands only relevant candidates, then uses their details as evidence. Users can query in natural language; with embeddings disabled, BM25 needs words matching the stored content, which may have been summarized in another language. Empty results call for alternative search terms rather than guessed facts. Retrieval limits and progressive disclosure do not delete or deduplicate stored history.

`sourcePaths` are maintained in a small SQLite provenance index across restarts. A consolidated memory's paths may only become available when expanded, through its original observations. The Agent's permission profile extends Codex’s read-only profile and enables the managed network proxy with only `127.0.0.1` allowed. Project `.env` reads are denied and project service keys are removed from its environment. Memory recall opens the provenance database read-only. These per-invocation settings do not edit the user's global Codex config. `aster context get <path>` reads the evaluation's temporary public snapshot and is intended for the extraction Agent; it is not a live remote Actor read API.

Memory capture is best effort. A compression timeout is logged and leaves the session open. Detailed retries and the memory lifecycle for real delegated execution remain deferred.

## Goals and work IM

The example includes the ongoing Knowledge Engine frontend TL Goal. `config.models` is an array of named entries (`name`, `provider`, `model`, `url`, `apiKey`); `config.goals.model` selects one globally. Supported Goal adapters are `minimax`, `anthropic`, and `openai` (chat completions). `${ENV_VAR}` credentials are resolved when the named model is selected. Goal reasoning uses pinned `@earendil-works/pi-agent-core` and `pi-ai` 0.87.1, independently of the memory LLM and System One model.

Top-level `goals` maps slugs to `{description, completionCriteria?}`. Omit completion criteria for a continuing responsibility. The Agent reads Contexts and memory, submits a progress/Signal plan, and actors apply it. Signals belong to SignalsRoot, with one originating Goal each. User messages received during planning are retained for the next evaluation. Ending a Goal deactivates future triggering; already-running delegations can still report.

Enable `contexts./lark.children./im` to observe recent messages from non-muted p2p and group chats. Each poll calls `im +messages-search` for a fixed time window, paginates its messages, then calls `im chat.user_setting batch_query` in groups of ten chat IDs. Muted chats are removed before child Actor updates and summarization. Missing or invalid notification settings fail the poll without advancing its cursor. Startup reads from midnight in `Asia/Shanghai`, or resumes today’s persisted retrieval progress with a one-minute overlap bounded by that midnight. Uninterrupted polling may finish the previous day’s tail across midnight. The cursor lives in private daily progress files, not public Context State. Fetched messages are durably journaled before the cursor advances. The default delay is 15 minutes after each completed polling round. Startup catch-up uses consecutive, serial query windows of at most one hour, committing each window before fetching the next. No full chat enumeration or per-chat history sweep is performed. Existing historical Contexts remain persisted; they are not deleted by this migration. Failed summaries retain their messages and retry locally after 30 seconds even if the chat receives no new messages. Startup actively restores today’s local pending summaries; prior-day backlogs remain untouched. Today’s startup summaries may initiate normal Goal screening.

Goal creation, relevant Context changes screened by System One, user messages, and execution results initiate planning. Signal extraction and whole-Signal readiness are separate checks. Newly planned Signals evaluate the source paths cited in the plan against the planning snapshot. Actual tasks run in isolated `~/.aster/tasks/{run-id}` directories through DoubaoWork. A saved session ID permits result polling after restart; an interrupted submission without an ID is marked uncertain and is not automatically repeated.

Open **http://127.0.0.1:4317** after `pnpm build && pnpm start`. The backend serves the separately built `apps/web/dist`; closing the browser does not stop the actors. To develop a replacement frontend, run `pnpm --filter @aster/web dev` alongside the backend.

Public loopback API:

- `GET /api/goals`: Goal Context snapshots with state and ordered messages.
- `GET /api/context?path=...`: a public Context, including source and execution evidence.
- `POST /api/goals/{slug}/messages`: JSON `{ "text": "..." }`, accepted into the Goal mailbox.
- `POST /api/goals/{slug}/end`: JSON `{}`, end an ongoing Goal.
- `POST /api/rpc`: typed queries and mutations from `@aster/api-contracts`, served as NDJSON RPC.
- `GET /api/events`: SSE `ready` and `invalidate` events. Invalidation payloads contain `{_tag: "Invalidate", keys: string[]}`. The browser refreshes matching AtomRpc queries; every ready/reconnect refreshes all mounted queries.

The server binds only loopback and checks Host/Origin. Model credentials are not exposed. The React/Vite frontend imports no Actor, Effect, or pi modules and has no filesystem access. Browser verification was unavailable in the development environment; API interactions, builds and actor tests were verified separately.

## IM rolling-summary core verification

Chat updates are batched for one second, screened by System One, and summarized through the replaceable `ChatSummarizer` service, implemented inside Lark integration and using `contexts./lark.children./im.config.summary.model`. Each message batch is partitioned by its message date in Beijing time. Both a daily summary and a rolling summary must be stored before their covered messages are removed. Group and direct conversations follow the same rules. Source screening now requires `stateChanged`; message-only updates do not invoke System One. Ingestion writes are excluded from evaluation, and polling cursors are private. Each summary commit journals both outputs before publishing them so an interrupted commit can be replayed without another model call. Existing inactive legacy Contexts are preserved; their old `through` field is removed when their Chat Actor next starts.

After `pnpm build`, run `node --use-env-proxy apps/local/scripts/verify-chat-summary.mjs` from the repository root. This uses synthetic evidence and real configured summary, System One and Goal models in isolated in-memory actors. It does not create a real Doubao task or require the managed memory process. A rejected relevance gate is reported as an automatic-chain failure; the script then separately checks Goal planning without changing production routing.

Follow the JSON event names `chat.summary.*`, `system-one.goals.*`, `goal.planning.*` and `delegation.*` by Context path in startup logs. Live tests reached Goal planning but also exposed inconsistent relevance classification in the configured Laya service; see `docs/im-summary-design.md`.

The app supplies `Models.configured` from `@aster/agent`, which reads `config.models` through the installed ConfigProvider. Lark summarization and Goal reasoning both use its pi-based `Agent.make({ name, tools })` and `agent.run({ messages })`. Each run is an independent conversation, and results are read from that run's tool messages. `summary.model` and `goals.model` can select the same model alias or different ones. The example and local YAML include the explicit summary selection.

## Unified tasks and approvals

Set `config.agent.model` to a name from `config.models`. This internal model prepares Tasks, extracts Signals and initializes Context descriptions. `config.goals.model` and Lark's summary model remain independent. `Signal.agent` selects a code-registered external executor: `codex` or `doubao-delegate`.

Run `pnpm build` and `pnpm start`. The local web app includes the approval queue: confirm a prepared Task, approve/reject an external request or answer its questions. The queue is also exposed as `GET /api/approvals` and `POST /api/approvals/respond` with `{ id, response: { decision: "approve" | "reject" } }` or `{ id, response: { text } }`; multiple questions accept `response.answers` keyed by question ID with arrays of strings. Existing loopback Host/Origin checks apply.

Queue state and messages persist under `~/.aster/actors/approvals/`; Run and Delegation records retain prepared Tasks and real external session/run IDs. On restart, actors are recreated and saved approval results are delivered by Actor path. Receiving a decision does not mean that the external task has completed; use “View task records” for response delivery and execution errors.

Codex requires app-server support. Doubao requires CLI status/wait support and a compatible Work desktop native response bridge. Unsupported approval controls remain recorded with an explicit delivery error; no synthetic chat approval or replacement session is sent.

An empty approval queue can mean no request was ever produced. Signal definitions alone do not create approvals: a Run must prepare a Task, pass readiness and use `confirm`, or an owned external execution must report a pending permission/question. Goal-generated Signals currently use `auto`. The dashboard distinguishes “no requests yet” from acknowledged decisions and surfaces the latest failed Goal evaluation so upstream failures are visible.

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

See [Reactive application API](../../docs/reactive-api-design.md) for the scoped HTTP/SSE implementation and browser query ownership.

The HTTP host composes internal `legacy-rest`, `static-assets`, `http-policy`, RPC and SSE modules. These are implementation modules, not new externally supplied Layers. Host/Origin and body-size checks wrap every route through the same policy.
