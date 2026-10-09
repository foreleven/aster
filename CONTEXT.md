# Local Signal Delegation

This context describes how activity from connected services and local tools becomes work for a local agent.

## Language

**Source Event**:
An occurrence received through a Channel, such as a chat message, ticket change, monitoring alert, or local tool activity. It does not by itself imply that local work should begin.

**Context**:
A coherent, independently addressable body of information relevant to identifying or pursuing work. Its outward-visible content consists of structured Context State and ordered Messages; chats, emails, workspaces, Signal definitions, and Signal runs are all Contexts.

**Context State**:
The outward-visible structured information about a Context, such as account information, mailbox information, or a Signal definition. It is distinct from the Context's ordered Messages and its implementation's private bookkeeping.

**Chat Summary**:
The current condensed understanding of a chat, carried forward from its previous summary and newly observed messages and exposed as part of its Context State. It retains relevant progress, decisions, blockers, pending work, and source references for deciding which Goals concern the conversation.

**Pending Chat Messages**:
Observed chat messages whose current content has not yet been incorporated into a durably saved Chat Summary. A newer edit remains pending even if an earlier version of that message has already been summarized.

**Chat Summary Request**:
A request to refresh one chat's summary from its accumulated Pending Chat Messages. Its message coverage is determined when an IM Agent Permit is granted, so messages observed while awaiting admission can join the same summary.

**Daily IM Retrieval Progress**:
The record of message retrieval coverage for a particular day, including unfinished coverage that may be retrieved on demand. Retrieval progress is distinct from completion of chat summarization.

**IM Retrieval Watermark (`through`)**:
The end of a retrieval interval whose fetched messages have been durably retained locally. It establishes retrieval coverage, not completion of daily or rolling summaries.

**Daily Chat Summary**:
A condensed record of a group or direct conversation’s discussions, decisions, and pending work based on messages from one calendar day in Beijing time. It is distinct from the Chat Summary, which carries the conversation's relevant understanding across days.

**IM Agent Permit**:
Admission granted from the shared IM capacity before a chat executes an Agent. It governs automated chat processing, independently of user confirmation for delegated tasks.

**Context Description**:
A fixed, human-readable explanation of a Context's basic identity, purpose, and relationship to the user, such as a Lark account used for work. It remains stable as the Context's state and content change.

**Memory**:
Information retained for later recall across observations and Agent sessions, associated with the Context from which it was learned. Examples include the user's work identity and work email address.

**Context Implementation**:
The Actor code that owns one Context and defines its public State and Message Schemas, Commands, behavior, and interfaces. Root implementations are registered in code, create their own children, and consume their own optional configuration subtrees. Public Context data has no type tag.

**Provider**:
A shared integration implementation that can supply one or more Context implementations with external capabilities and resources. For example, a Lark Provider can supply separate IM and Mail Channel implementations.

**Message**:
An ordered, retained item of Context information, such as observed activity, a user decision, or a delegation result. Retention depends on the Context; a Message may be removed after its information has been incorporated into a durable summary.

**Summary Checkpoint**:
A durable, compressed representation that replaces a prefix of a Context's Message history. Messages covered by the checkpoint no longer need to be retained; the checkpoint never replaces exact structured state.

**Context Command**:
A transient request delivered to a Context actor asking it to perform an operation. A command is not itself persisted, may be rejected, and may produce zero or more Messages.
_Avoid_: Message

**Workspace Context**:
A Context representing a working directory on the user's computer and relevant local activity.

**Channel**:
A data-ingress behavior of a Channel Context, such as Feishu IM, Feishu Mail, or workspace discovery. It may declare paths for child Contexts it produces; the Channel Context may also have Messages.

**Email Context**:
A Context representing one email in one mailbox, identified by that mailbox's email ID. Repeated observations of the same email address the same Context.

**Mailbox Daily Index**:
The default listing of emails for the current calendar day in a mailbox's time zone. Leaving this listing does not remove an email retained as evidence for assistant work.

**Mail Retrieval Watermark (`through`)**:
The end of the interval covered by automatic mail retrieval, whose observed emails have been durably retained. It describes retrieval coverage, not completion of assistant work, and is independent of on-demand historical queries.

**Late-arriving Email**:
An email newly appearing in a connected mailbox whose date precedes the mailbox's completed retrieval interval. It is distinct from historical mail already present when the mailbox was first connected.

**Goal**:
A continuing assistant responsibility pursued across conversations, observations, and actions, with Tasks tracking individual pieces of work and Signals monitoring conditions for action. A Goal can be paused, resumed or manually deleted; completion belongs to a Task, not to the Goal itself.

**Goal Summary**:
The current understanding of a Goal's progress, established findings, outstanding work, and relevant prior outcomes. It provides continuity across successive planning conversations.

**Goal Conversation**:
The user-visible exchange between the user and the Goal's assistant, communicating requests, observations, progress and conclusions in natural language. Tool calls, their arguments and raw tool results are not part of this conversation.

**Goal Agent Session**:
The isolated durable Agent execution space owned by one Goal. It contains that Goal's primary Conversation and its Agent Runs; a Goal does not share this session with another Goal.

**Agent Run**:
One model planning execution within a Goal Agent Session, including its model messages, tool interactions, and structured planning result. It is distinct from a Delegation, which performs external work.

**Evaluation Result**:
The structured result of an Agent Run, containing the planning conclusion and proposed Goal Task or Goal-owned Signal changes. The GoalActor validates and applies it; the result is not itself an authorization for external execution.

**Goal Input**:
An accepted, ordered input to a Goal evaluation, such as a Goal Intent, User Input, Signal Occurrence, Execution Feedback, startup recovery, or retry request. It is persisted by the Goal before being handed to the Agent Session.

**Goal History**:
The retained messages of a Goal, including its public conversation and internal evidence or work feedback. The public conversation and execution details are views of this history, which remains available beyond the portion used for current reasoning.

**Goal Feed**:
The chronological view of a Goal's ongoing activity drawn from its History, including observations that do not lead to a task.

**Goal-relevant Context Change**:
A change to another Context that contains evidence capable of changing a Goal's progress, blockers, work or conclusions. It is evidence for the Goal's assistant, which decides whether to communicate a natural-language update; the source change is not itself an assistant statement.

**Goal Screening**:
The independent relevance assessment of one source Context change against one Goal before it can enter that Goal's history. Goals do not compete for selection, and screening does not itself become a Goal activity record.

**Goal Relevance Score**:
A calibrated continuous score in `0..1` expressing how strongly a source Chat Summary contains evidence relevant to one Goal. It is evaluated independently for each Chat–Goal pair; it does not rank Goals against one another or express urgency.

**Screening Rationale**:
A bounded explanation accompanying an admitted Goal Relevance Score, stating why one source Context was judged relevant to one Goal. It is part of the Goal activity shown to the user, not an authorization or execution instruction.

**Goal Intent**:
A durable, admitted input to one Goal Agent evaluation, identifying the source Chat, its persisted summary content, the independent relevance score, and the screening rationale. It is evidence for Goal planning, not a Task, Signal, or execution authorization.

**Goal Evaluation**:
One Goal Agent planning exchange that considers an ordered batch of admitted Goal Intents and current Goal state and may produce a conclusion together with Task or Goal-owned Signal changes. Intents arriving during that exchange belong to a subsequent evaluation, and its outcomes are not mutually exclusive.

**Goal Evaluation Group**:
The Timeline projection that explicitly links one Goal Evaluation's ordered input Intents to its conclusions and Task or Signal changes. Later execution results are separate items linked back to the originating Evaluation and Task.

**Evaluation Handoff**:
The durable transfer of an accepted Goal Input from its Goal to the Goal Agent Session. Its pending, running, failed, or reconciliation-required state describes whether planning was delivered and processed; it does not describe external Task execution.

**Evaluation Retry**:
A new Goal Evaluation Group that reprocesses the same durable Intent batch after a failed planning exchange. It points to the failed group and preserves both groups as separate history.

**Screening Decision Record**:
The structured audit record of one Goal relevance assessment, including its score, admission result, normalized screening input snapshot, input/version fingerprints, policy and model metadata, and outcome. It is retained in a separate local append-only JSONL dataset for evaluation and calibration, outside Goal History; raw message batches, credentials, and unrelated Contexts are excluded.

**Goal Task**:
A Task pursued on behalf of a Goal. It is the same work concept as Task, not a separate planning record paired with an external execution Task.

**Signal**:
A standing Context that monitors a condition, authored by the user or generated by an Agent for its Goal. A Goal-owned Signal reports a match to its Goal for assessment, which may record only an observation; an independent Signal specifies delegated work and its execution mode.

**Signal Occurrence**:
A particular matching of a Signal's condition, together with the evidence supporting that match. For a Goal-owned Signal it prompts Goal assessment, without itself requiring a task or authorizing execution.

**Signal Run Context**:
The independent Context created as the record of one triggering of a Signal. It can refer to the other Contexts that informed that triggering.
_Avoid_: Activation

**Execution Mode**:
The choice in a Signal definition of whether a triggering starts local work directly or waits for human confirmation.

**Execution Readiness**:
The determination of whether a particular Signal Run Context can be delegated now, needs more user input, or cannot currently be executed. It is separate from the Signal's confirmation requirement.

**Confirmation**:
The user's authorization for a particular execution proposal to initiate a Delegation. It does not authorize a revised proposal or future executions of the same task or Signal.

**Task**:
A concrete piece of work that retains its identity as instructions, relevant evidence, follow-up input and outcomes accumulate. It can be carried out by an internal Agent or delegated to an external Agent; delegation is not required for work to be a Task.

**Task Completion**:
The point at which a Task's currently accepted work has been fulfilled. Later instructions concerning the same work can reactivate that Task with its identity and accumulated context intact; distinct work belongs to a new Task.

**Task Follow-up**:
Additional instructions concerning an existing Task. While work is underway, they guide the ongoing execution at its next supported opportunity; after completion, they can reactivate the same Task.

**Delegation**:
An external Agent's execution of a prepared Task, tracked by its requesting execution record.

**Delegation Context**:
A Context representing one Delegation, including its external session, progress and outcome.

**Approval Queue**:
The shared collection of pending requests for user decisions or input needed to advance work. Each response returns to the invocation that raised the request; permission requests and requests for missing information remain distinct.
