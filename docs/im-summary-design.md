# IM chat summaries and Goal routing

## IM frequency control (implemented)

The requested default retrieval interval is 15 minutes. Startup catch-up for today splits today's outstanding coverage into query windows of at most one hour, preserving the existing today-only retrieval scope rather than limiting history to the most recent hour. Fetch these windows consecutively and serially, durably storing each window and advancing its progress before proceeding. Do not wait fifteen minutes between catch-up windows. After catching up to the current time, wait fifteen minutes after each normal polling round completes before starting the next round, preserving single-round execution. Restart immediately catches up from today's saved progress rather than waiting for a persisted polling timer. For example, a normal round starting at 10:00 and completing at 10:02 is followed by the next round at 10:17.

All chats share Agent admission control configured at the IM level. Each chat must acquire an IM Agent Permit before executing an Agent. Daily and rolling summaries each require a separate permit.

Before requesting summarization Agent admission, use System One to assess the new messages together with the existing summary and decide whether an update is warranted. Sparse messages may still carry important decisions; message count alone must not determine whether to summarize. If System One decides to defer, retain the pending messages without advancing summary coverage. Reassess the combined pending batch when new messages arrive; without new messages, do not repeatedly invoke System One for the same deferred batch. This gate is distinct from the existing post-summary System One screening for Goal relevance.

During uninterrupted operation, day-end triggers summarization for chats that still have pending messages, including batches previously deferred by System One. This day-end flush proceeds through the shared IM Agent admission queue and uses the original Beijing message date. It does not repeatedly ask System One to approve an unchanged deferred batch. If the application was stopped and missed day-end, prior-day backlogs remain unprocessed under the existing recovery policy.

Accepted summarization Agent admission defaults: across IM, Agent starts must be at least ten seconds apart, with at most two Agent runs active concurrently. Both limits are configurable at the IM level. Each daily-summary run and each rolling-summary run acquires its own permit; a failed run must acquire a new permit before retrying. Requests wait in the shared queue when capacity is unavailable. System One does not acquire these permits and has no added rate or concurrency limits; the proposed separate System One limiter was rejected.

If the pre-summary System One call fails, retain pending messages and retry the judgment after thirty seconds. A failed call is not a decision to defer summarization. Only an explicit decision that no summary update is needed waits for new messages; the accepted day-end flush still enters the Agent queue.

Each chat has at most one waiting Agent admission request. Messages arriving while it waits accumulate in its pending batch rather than creating additional queue entries. On admission, select and freeze the batch, partitioned by Beijing message date. Messages arriving after execution begins remain pending for the next cycle. The daily and rolling summaries for a selected batch cover the same frozen messages, even though each Agent execution acquires its own permit.

On restart, reconstruct unfinished admission requests from today's durable local processing state. Persist the latest global Agent start time so restart cannot bypass the ten-second minimum start interval. Persist System One deferral together with the batch it assessed: unchanged deferred batches remain waiting for new messages rather than being judged again merely because the process restarted. Prior-day backlog recovery remains out of scope.

Agent admission uses FIFO order among waiting requests. After its daily-summary run completes, a chat requests a separate rolling-summary permit at the queue tail. Failed runs also re-enter at the tail when eligible to retry. A chat does not reserve capacity across both runs or gain priority merely because it has more pending messages.

Persist successful daily-summary output as a stage checkpoint together with its frozen message batch. If the subsequent rolling-summary run fails or the process restarts, reuse that output and resume only the unfinished stage through normal Agent admission. Do not call the daily-summary Agent again for the same completed stage. Raw messages remain retained until both summaries are successfully saved; newer arrivals and edits remain pending for a later batch.

During uninterrupted operation, trigger the day-end flush only after a successful cross-midnight retrieval has durably covered the previous day's final interval. For example, after a 23:50 poll, the 00:05 poll first retrieves the remaining pre-midnight messages, then queues yesterday's outstanding summary work. A failed retrieval does not declare day-end coverage complete or prematurely trigger the flush. This avoids summarizing an incomplete daily batch at midnight and immediately regenerating it after the remaining messages arrive.

The implementation uses `pollIntervalMs: 900000`, `catchUpWindowMs: 3600000`, `summary.agentStartIntervalMs: 10000`, and `summary.agentConcurrency: 2` beneath the IM configuration. The root app supplies `ImSummaryGate` using its configured System One client, including when IM is enabled without Signals or Goals. Lark owns the summary-need prompt. A shared `ImAgentQueue` enforces admission, and `agent-admission.json` under `~/.aster/im` stores the last start time. Chat daily JSON now includes an assessment fingerprint/decision and a frozen stage checkpoint with the successful daily output. The shared FIFO is rebuilt from today's durable work on restart; previous process queue positions are not retained. Idle Chat Actors stay alive while the IM process runs so receive-timeout passivation cannot cancel queued work or lose day-end work. Earlier implementation and verification sections below describe the preceding daily-storage release.

Status: the first core flow is implemented and exercised. A real-model smoke test reached Goal planning automatically, but earlier runs exposed unstable System One relevance decisions. External delegation was not performed by the live diagnostic.

Frequency-control verification: `pnpm typecheck` and `pnpm test` pass, including the workspace build and all 108 tests. Eight new frequency-control tests cover hourly catch-up and per-window recovery, fifteen-minute polling delay, shared FIFO spacing/concurrency/cancellation, persisted admission timing, strict System One decisions, deferred-batch recovery, queued-arrival merging, daily-stage reuse after restart, System One failure retry, and cross-midnight flush ordering. The synthetic live script passes `node --check`; this implementation was verified with local fake clients and temporary storage, without fetching real work messages or running live models.

## Requested direction

- Each `/lark/im/chats/{id}` Context exposes its current chat summary in `state`.
- Changed messages are accumulated and summarized by a model in batches.
- Messages incorporated into a successfully stored summary can be removed. The first batching and commit policy is described below.
- Changes to the resulting state pass through System One to match relevant Goals; the Goal model then decides the work to execute.
- Add logs at the key stages so an incoming chat change can be followed through summarization, relevance screening, Goal reasoning, and execution.
- Ordinary chat updates remain distinct from writing long-term Memory. This proposal does not change the existing Signal-triggered activity capture policy.

## Accepted daily retrieval direction (2026-09-29; implemented, verification below)

At startup, inspect the saved `through`. Default retrieval is limited to today: without a cursor for today, retrieve from today's start to the current time; with a cursor for today, resume from that progress. Any overlap used for startup retrieval must respect today's lower bound. For example, restarting at 10:00 after stopping yesterday at 18:00 retrieves today's messages from 00:00; it does not automatically retrieve yesterday's remaining messages.

Persist retrieval progress for each day in local files. Preserve historical gaps as unfinished coverage, available for on-demand handling rather than automatically backfilling them at startup. The on-demand interface and recovery mechanics remain to be designed.

The scope is the current single-account IM integration. Keep one shared IM retrieval progress record per day, matching the existing search across conversations; do not create a separate retrieval watermark per chat or introduce an account dimension. Track each chat's summary processing independently, so a failed chat summary does not block retrieval or other chats' summaries.

Move `through` out of public IM and Chat Context State into the local retrieval progress files. Retrieval bookkeeping is private operational state; Goal screening reads chat information and the rolling summary without polling-cursor changes contributing to public state changes. This supersedes the earlier decision to temporarily keep `through` in public state.

Use `Asia/Shanghai` (Beijing time) to define today and the date of daily retrieval progress and chat summaries, independently of the machine's local timezone. A day runs from 00:00 inclusive to the next day's 00:00 exclusive in that timezone.

During uninterrupted operation, polling continues across midnight from the previous retrieval position, including the previous day's final interval; the today-only lower bound applies to startup, not to each poll. For example, a poll spanning 23:59:40 to 00:00:40 retrieves both sides of midnight and attributes retrieval coverage to the respective days.

Before daily summarization, partition messages by their message timestamp in `Asia/Shanghai`, not by retrieval time or summarization completion time. A batch spanning multiple dates updates a separate Daily Chat Summary for each date, using only messages belonging to that date. A message exactly at 00:00 belongs to the new day. Retries or summaries finishing after midnight retain the original message-date partition. The rolling Chat Summary continues to span dates.

Store each day's summary of each chat in a local Markdown file with frontmatter. The Daily Chat Summary covers only that day's messages, recording discussions, decisions, and pending work. It coexists with the existing rolling Chat Summary, which continues to carry relevant understanding across days for Goal screening. Daily archives do not replace the rolling summary or reset its cross-day context. Group chats and direct conversations use the same daily Markdown archive, progress tracking, retry, and message-retention rules; daily archives are not limited to groups.

Update the same daily chat Markdown file incrementally as new messages arrive during that day; generation does not wait until midnight. Retain the file after the day ends. On-demand retrieval of historical messages updates the file for the corresponding message date rather than adding those messages to today's summary.

Daily chat Markdown frontmatter contains the date, chat ID, chat name, update time, and summarized coverage. Persist operational bookkeeping—failure status, retry information, and pending messages—in separate local JSON files. Recovery must not depend on reconstructing that bookkeeping from the human-readable summary file. The implemented field names and file layout are listed below.

Advance `through` once the retrieval interval has completed and its fetched messages have been durably stored locally; summary success is not a prerequisite. Persist pending summarization separately and retry failed summaries from local messages. Raw messages may be removed only after both the Daily Chat Summary and rolling Chat Summary covering them have been saved successfully. A failure in either summary must retain the messages needed for recovery. This replaces the earlier single-summary removal condition for the planned daily-summary flow.

Today's messages retrieved at startup are eligible for the normal summary-state-driven Goal screening flow, including messages sent earlier today while the application was offline. Replace the current blanket bootstrap suppression for this case; eligibility does not itself guarantee a Goal match or task execution. On-demand retrieval of messages from earlier days updates summaries by default without automatically initiating tasks. Historical summary updates must preserve this suppression through retries rather than treating recovered work as fresh live activity.

Historical backfill is deferred. Its future design must specify how historical messages revise the current rolling summary without regressing newer facts or automatically initiating tasks.

### Observed summary recovery gap

Inspection during this design session found 5,183 persisted chat Contexts: 4 had `state.summary`; 5,147 had pending messages but no summary, and 32 had neither. The State inspector renders the full state without filtering summary fields. The current schema makes `summary` optional, and only a successful summary commit creates it. Loading persisted Contexts does not itself start their Chat Actors: IM polling creates actors for chats returned in the current search, and the pending-message recovery hook runs only when a Chat Actor starts. Thus quiet persisted chats can remain unsummarized across restarts. These observations establish a recovery gap, not the individual failure history of every unsummarized chat.

Accepted recovery scope: at startup, proactively resume locally pending summarization for messages belonging to today in `Asia/Shanghai`, without waiting for new messages in those chats. Leave prior-day message backlogs retained and unprocessed for now; a newly activated chat must not fold that backlog into today's summary batch. This does not cancel the accepted completion of cross-midnight polling and summarization during uninterrupted operation. Historical backfill and backlog processing are deferred from this implementation slice.

## Accepted rolling summary

The summarizer reads the previous summary together with a fixed batch of new messages and updates the current understanding of the conversation, including progress, decisions, blockers, pending work, and key source references. Once the replacement summary is successfully stored, only messages covered by that batch are removed. Messages received during summarization remain for the next batch.

## Accepted state-driven screening

Across Context actors, public `state` changes are the trigger for System One screening. Appending or removing `messages`, updating a description, and changing private actor bookkeeping do not independently trigger System One. Writing an equivalent state does not constitute a state change. This replaces the earlier generic Context-content-change trigger, rather than being an IM-only rule.

For chats, raw message changes request summarization; a changed summary in state requests screening. Existing restrictions on execution feedback and arbitrary Signal-to-Signal discovery need to remain explicit when defining routing; state-driven screening does not itself authorize recursive execution.

## Verified current behavior

Before this change, `LarkChatActor` stored chat metadata and a polling cursor in state, merged messages by message ID, and evaluated changed non-bootstrap pulls without summarization.

Before this change, the application evaluated active Signals directly before calling System One for Goal relevance. Matching Goals receive an Evaluate command. Goal reasoning can create or adjust Signals under SignalsRoot, whose runs delegate to Doubao. Whether the new IM route replaces the direct Signal evaluation path is still an open decision; independent Signals must be considered explicitly.

The registry now publishes `stateChanged` for every Actor Context. Public updates remain visible to the UI and memory policies, but source screening requires an actual state change. Existing source eligibility and execution feedback restrictions remain; internal Goal/Signal progress does not recursively initiate source discovery. Goal relevance receives path, description and state, excluding pending messages.

## Next decisions

1. Summary contents and retained references after raw messages are removed.
2. Batch timing and model selection.
3. Initial state/bootstrap screening and moving polling bookkeeping out of public state.
4. Relationship between Goal matching and direct evaluation of independent Signals.
5. Log fields and the visibility of the end-to-end processing outcome.

## First implementation and verification

The user chose to run the core flow first, deferring migration of the polling cursor. For this slice, a Chat batches updates over one second and uses a replaceable ChatSummarizer service. The original slice used an app adapter and `config.goals.model`; this was replaced by the ownership adjustment below. The rolling summary has `text` and source `references` (message ID and URL). The actor stores summary and message removal together through the existing Context store. Failure retains the batch for a later poll to retry; successful completion removes only unchanged messages actually covered by that batch. New arrivals and edits remain pending.

`through` remains in public state as requested. Ingestion writes retain `evaluate: false`; summary commits initiate screening unless they are bootstrap history or the state is unchanged. Recent in-memory message fingerprints avoid overlapping polls immediately replaying compacted messages. Persistence of this fingerprint bookkeeping across actor lifetimes is deferred; a restart can reobserve the polling overlap.

The application screens source state against Goals before evaluating independent user-authored Signals. Goal-derived Signals are considered through Goal planning/reconciliation, rather than also executing from the earlier direct source branch. Existing initial Goal evaluation and execution feedback remain.

Logs include `chat.messages`, `chat.summary.started/saved/failed`, `system-one.goals.started/matched`, `goal.planning.started/completed/failed`, and `delegation.started/submitted/finished`. Paths connect stages; normal logs avoid raw chat content and credentials.

Run `pnpm build` then `node --use-env-proxy apps/local/scripts/verify-chat-summary.mjs` for a synthetic, isolated real-model diagnostic. It does not start managed memory, read work conversations, or delegate an external task. If System One rejects the fixture, it explicitly records the failed gate and tests Goal reasoning separately; that separate test is not evidence that production routing succeeded.

The initial real-model run successfully summarized two synthetic project messages and removed them from the pending list. The configured Laya service nevertheless classified the project risk as unrelated (the original run returned `no` with probability 0.9708). Alternative phrasings also produced false positives on unrelated dinner conversation, so changing the prompt merely to pass the example was not adopted. Separate Goal reasoning returned a plan. Relevance quality needs further validation before relying on unattended work-IM routing.

A final rerun using the unchanged production Goal-relevance prompt matched the synthetic chat to the Goal and automatically reached Goal planning. Two messages were compacted to zero pending messages, and the Goal produced three candidate Signals. This establishes the core model/actor route on that run, not classification reliability: an earlier summary of the same source messages was rejected. No candidates were installed or externally delegated by this diagnostic. The full offline suite passes 65 tests (28 Actor, 9 Context, 6 memory, 22 application), including covered-message removal, arrivals during summarization, retry after failure, unchanged-summary suppression, and state-only change detection.

## Implemented ownership adjustment

The user requested moving chat-summarizer implementation into Lark integration and extracting a shared Agent package. The app supplies models; Lark summarization uses `Agent.make({ name, tools })` and supplies messages separately to `agent.run({ messages })`. Lark now owns the summary prompts, tools and output validation, and consumes `contexts./lark.children./im.config.summary.model`. The app provides `Models.layer(config.models)`; the shared Agent package thinly wraps pi. Both summary and Goal reasoning have migrated. See [Agent capability design](agent-design.md) for the accepted contract and verification.

## Daily retrieval and summary implementation (2026-09-29)

Lark owns an injectable `ImStorage` service rooted at `~/.aster/im`. Each date has `progress.json` with merged successful retrieval intervals and `through`, `<chat-id>.md` for the daily summary, and `chats/<chat-id>.json` for pending messages, completed fingerprints, the daily checkpoint, retry metadata, and an optional prepared summary commit. Markdown frontmatter uses `date`, `timezone`, `chat_id`, `chat_name`, `chat_mode`, `updated_at`, `first_message_at`, `last_message_at`, and `message_count`. Writes use temporary files, fsync, and atomic rename.

The IM Actor journals all fetched batches before advancing the daily progress, then notifies Chat Actors. It restores today's private pending inboxes and imports only today's legacy pending messages, without requiring search hits for those conversations. Legacy import cannot overwrite a newer journaled message or reintroduce a completed fingerprint. Old public cursors are not trusted to establish coverage in the new store: without daily progress, the first upgraded run reads from today's midnight. Inactive legacy Contexts are preserved; their old public `through` is removed when their Chat Actor starts.

Chat Actors split batches by message date and call the same summarizer service in daily and rolling modes. Both outputs are journaled as one prepared commit before writing the daily Markdown and public rolling state. Only after both saves succeed are the covered, unchanged messages removed from pending storage. An interrupted commit is replayed on recovery without invoking the model; newer arrivals and edits remain pending. Failure retains raw messages and records `lastError`/`retryAt`, with a local 30-second retry. Continuous actors can finish previous-day work after midnight; restarting resumes today only. Daily history and fingerprints remain on disk for deduplication across restarts.

Historical retrieval commands are not part of this slice. The ordinary retrieval path preserves the existing muted-chat filter. No live work conversations or external tasks are needed for the automated verification.

Verification: `pnpm typecheck` and `pnpm test` pass, including the full workspace build and 100 tests (7 Agent, 30 Actor, 19 core, 6 memory, 9 integrations, 29 application). IM coverage checks startup bounds, cross-midnight polling and partitioning, durable ingress before cursor advancement, quiet-chat recovery, preservation of historical pending messages, interrupted summary-commit replay, failures in daily/rolling summarization, automatic retries, unchanged-state suppression, concurrent arrivals, persistent deduplication, and legacy import preserving newer edits. The synthetic live-verification script passes `node --check`; no live model calls, real IM retrieval, or task delegation were performed for this change.
