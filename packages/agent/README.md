# Agent

A thin Effect wrapper around pi 1.0.0. Message and tool types are re-exported from pi; core and business integrations own their respective prompts, tools and business-result validation. Goal evaluations can opt into `pi-durable` 1.0.0 by passing a stable `sessionId` and `requestId` to `Agent.make`. The adapter stores one JSONL conversation per Goal, resumes unfinished work, and deduplicates retried submissions; calls without that option retain the isolated `pi-agent-core` path.

Durable Goal sessions use `~/.aster/goals/<sessionId>/pi` by default. Tests and hosts can provide `storageDirectory` to place the conversation elsewhere. The Goal Actor persists accepted business inputs before invoking the adapter, so a failed handoff can be retried with the same request identity.

Personal processing selects `owner: "personal"`, which places its conversation under `~/.aster/personal/<sessionId>/pi` and uses a separate extension identity. Existing Goal storage paths and extension names are preserved.

Pi 1.0.0 creates its root conversation with `ownership: { kind: "ownerless" }`. Goal/Personal conversations already use this durable ownership model; short Task executions use the separate task-owned runtime below. Aster additionally persists an `app.aster.agent.exchanges` document and accepted/result entries. Admission freezes the request payload, instructions, result-tool contract, tool/model configuration fingerprint and explicit catalogue version before submission can resume scheduling. Reusing an identity with changed input/configuration or another owner fails before execution. Only the admitted unfinished exchange may resume; a new request cannot overwrite its instructions.

A settled exchange records the exact native result-entry identities on the same Pi transaction line before invoking caller callbacks. Replay reads those immutable entries without submitting work, reconfiguring the conversation or starting the scheduler. Known unanswered/missing-result failures also persist and release admission for a new explicit attempt; unknown submission/storage/interruption outcomes retain the original pending identity. Callers must bump `catalogueId` when changing tool or context-transform behavior. This document is private recovery state, not a public Context or UI transcript.

`AgentError.outcome` is `failed` only when the durable adapter has committed a terminal failed exchange (including replay of that recorded failure). Other durable failures return `unknown`. Domain callers may create a new request for the former; they must retain and reconcile the original request for the latter. Local Effect interruption remains interruption. Native terminal failure detail is retained with the error, including a failed context budget guard.

Durable callers may supply `contextBudget: { contextTokens, reserveTokens }`. The adapter validates it, caps the context window to the provider model, and freezes the effective budget with request admission. Pi's native blocking compaction runs at that window minus the output reserve, retaining roughly half the input budget verbatim. It writes `pi.compaction` summaries in the same conversation and continues the original generation; it does not create another Aster exchange. Background compaction is disabled for these bounded invocations so compaction remains owned by the run. Unknown tool outcomes still fence generation and compaction, and interruption retains native recovery checkpoints. Budget changes require a new request; they cannot silently change an admitted pending exchange.

Both durable execution paths classify tool outcomes with the same policy. Read-only tools and pure result tools explicitly declare safe replay. An unsafe tool makes its entire round sequential; an uncertain result fences subsequent tools, including sibling calls in the same model answer. Native pre-execution validation failures and host-declared `rejectedToolResult(message)` allow model correction. A handler may declare rejection only when no side effect was accepted. Unsafe thrown errors, unclassified error results and interrupted unsafe intents retain uncertainty. Goal/Personal exchanges remain pending; task-owned executions expose an `unknown` result. Neither path automatically repeats the unsafe action.

Pi 1.0.0 reports ordinary generation-hook failures and continues. Aster therefore carries each hook validation to its provider callback using the native invocation signal. The provider observes that validation before invoking the configured model, so uncertain-tool recovery and failed context guards cannot silently continue ordinary generation. Native compaction declines when its conversation retains an unknown tool outcome. Recovered summarize/retry phases skip the selection hook; their provider calls therefore check retained outcomes across the whole owner before contacting the model. Pi does not expose a conversation identity at this provider boundary, so an unattributed model request is conservatively blocked if any conversation on that owner has unknown work. Explicit reconciliation of unknown operations and approval-aware execution remain separate work.

Replaying a settled request returns only its input-to-answer transcript range, read with pagination. A terminating tool round includes its tool results even though Pi identifies the preceding assistant tool-call entry as the answer. Later requests and their results are excluded, including when providers reuse tool-call IDs. `transformContext` runs through Pi's generation hook before each provider request. Interruption cancels the local wait and joins the invocation through `Harness.close` before the Effect scope releases; it does not claim that external work was cancelled. The storage lease remains held through that entire drain, including caller cancellation.

```ts
import { Agent, Models } from "@aster/agent";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const agent = yield* Agent.make({ name: "summary-model", tools: [] });
  return yield* agent.run({
    messages: [
      { role: "system", content: "Summarize the supplied evidence.", timestamp: Date.now() },
      { role: "user", content: [{ type: "text", text: "Evidence…" }], timestamp: Date.now() },
    ],
  });
});

// The app supplies its named model configuration once.
// Effect.runPromise(program.pipe(Effect.provide(Models.layer(config.models))))
// ActorSystem.make().pipe(ActorSystem.provide(Models.layer(config.models)))
```

`Agent.make` resolves `name` through the Models Effect service. Every `run` creates a fresh pi instance, including concurrent runs on the same handle. Returned `{ messages }` contains only generated messages, not the supplied input. Business outputs can be returned in a native pi tool's `details`, with `terminate: true` when appropriate, then read from the resulting `toolResult` message.

A terminal pi error becomes `AgentError` with the generated messages. Ordinary tool failures remain in pi's loop and can be corrected by the model. Effect interruption calls `abort()` and waits for pi to become idle; it remains interruption rather than becoming an ordinary failure. Callers apply `Effect.timeout` for their own execution limits.

Models supports the existing MiniMax, Anthropic, and OpenAI chat-completions providers, with configured URL, model ID and credentials. Environment credential references are resolved when a model is selected. Neither this package nor its callers add a second message or tool protocol.

`Models.configured` reads the typed `config.models` array through Effect ConfigProvider. `Models.layer(entries)` remains available for explicit composition and tests. Both capture the provider during acquisition; exact `${ENV_VAR}` credentials resolve through its `secrets` namespace when a model is selected. The shared `secretConfig` retains values as `Redacted` until the SDK boundary. Unused model credentials are not required, and prompt text is never interpolated.

## Durable task execution

`PiDurableAgentRuntime.make` acquires a scoped Harness from an injected storage opener and returns Effect-based `submit`, `lookup`, `status`, `resume`, and `wait` operations. It does not import Aster domain commands. A caller supplies an owner ID, resolved model, and versioned tool catalogue identity. The host owns the storage lease. Shutdown joins Harness.close; cancelling a wait only cancels observation, while accepted submit transactions drain.

Each stable request atomically creates an `app.aster.execution` task, its task-owned conversation, configured model/instructions, and an accepted history entry. The frozen input includes prompt, instructions, provider/model, catalogue ID and environment policy ID. An identical request returns the same real conversation/task identifiers; changed reuse is rejected. A terminal business result is committed with its history entry. Terminal replay does not invoke the model again. Reopening refuses a different owner, or changed model/catalogue/environment policy for pending work; hosts must version these identities when changing semantics or authority.

Safe tools may resume after an interrupted intent. Unsafe tools use Pi's unsafe replay policy, and interrupted/error results fence all subsequent tool calls in that execution. The public result remains `unknown` until an explicit reconciliation flow exists. Native frames stay private. Hosts may supply native Pi tools and an ExecutionEnv factory; the factory receives only the admitted Task evidence and a stable namespace, checked against the persisted policy on every use. These native tools share the same replay/unknown-outcome fencing as AgentTool adapters. The production integration supplies only a prepared-evidence read tool, described in its README. Goal/Personal remain ownerless and use their separate `Agent.make({ durable })` boundary.

The runtime also owns the infrastructure-only `withSession` bridge used by PiDurableContext. Short Context/execution transactions share a permit with owner recovery; task waits remain outside that permit. `recover` drains and closes the retired Harness before acquiring a fresh storage handle, revalidates owner/model/catalogue identity, and replaces the driver behind existing business handles. Old observers fail explicitly and must reconcile their retained handle; they never resubmit work. Failed reopen leaves admission fenced. The enclosing Scope closes the current owner exactly once, after admitted mutations drain.

`lookup` searches retained task admission by request ID and verifies the original prompt/instructions. It returns an Option handle without creating tasks/conversations, appending admission entries or resuming the Harness. Missing means no retained match, not permission to submit again. Completed admissions remain inspectable after the configured model/catalogue changes; submit still compares the complete frozen configuration, and pending recovery still requires its original configuration.

## Local Pi storage ownership

All production Pi openers share `PiStorageLease`: ownerless Goal/Personal exchanges, Context-only Sessions and shared Context/execution Harnesses. It uses Node 24 built-in SQLite with a nonblocking exclusive transaction on a permanent `.aster-owner.sqlite` file in the canonical storage directory. This provides a kernel-enforced local-filesystem lock across processes and symlink aliases; no extra native dependency or PID-file deletion protocol is used. Competing owners fail before opening Pi or invoking a model. The OS releases ownership after process death, and the next owner replays authoritative Pi storage normally. Never remove the lock database while an owner may be live. Network filesystems and distributed lease failover are not supported.

Acquisition and release belong to Effect Scope. Native synchronous calls are confined to the lock boundary because Effect FileSystem does not expose advisory locks. Pi closes before lease release; failed Harness/Session close quarantines the lease until process exit, rather than admitting a replacement writer while drain is uncertain. Retired lease handles reject reuse. `.aster-owner.json` is a diagnostic descriptor, not lock authority. `RuntimeSnapshot.storageOwners` exposes owner identity, random lease identity, hashed storage identity, PID and held/quarantined status without directory paths. Ownerless JSONL opens now explicitly request fsync.

## Durable reconciliation

`durable.reconcile` inspects existing exchanges and native submission status. Saved results replay across prompt/catalogue changes; normal request replays still require the exact admitted payload. Previously accepted uncertain requests cannot issue fresh provider calls during reconciliation. `durable.replayOnly` reads legacy saved results without admitting or resuming work. Missing receipts fail explicitly; unsettled receipts remain unknown. Neither mode reconstructs a legacy input from current mutable state.
