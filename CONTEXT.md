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
An ordered, persisted event in a Context's history, such as observed activity, a user decision, or a delegation result. Messages are produced by handling Commands and are replayed during recovery.

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

**Goal**:
A user-defined desired outcome or ongoing responsibility pursued across conversations, observations, and actions, with Goal Tasks tracking work and Signals monitoring conditions for action. A Goal with completion criteria can complete when the evidence satisfies them; a Goal without completion criteria remains active until the user ends it.

**Goal Summary**:
The current understanding of a Goal's progress, established findings, outstanding work, and relevant prior outcomes. It provides continuity across successive planning conversations.

**Goal Conversation**:
The ordered conversation through which the user and Agent pursue a Goal, preserving their exchanges and the evidence used during reasoning. It includes important observations and conclusions even when no task is created.

**Goal History**:
The complete retained record of a Goal's conversation and progression, including milestones, conclusions, task and Signal decisions, and execution feedback. It remains available beyond the portion used for current reasoning.

**Goal Feed**:
The chronological view of a Goal's ongoing activity drawn from its History, including observations that do not lead to a task.

**Goal Task**:
A persistent piece of work tracked by a Goal, retaining its identity as evidence, plans, and execution outcomes accumulate. The Goal Agent can mark it complete while associated Signals continue monitoring for subsequent changes; its completion is distinct from an individual execution's outcome.

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
A concrete unit of work prepared for an external Agent from a Goal execution proposal or an independent Signal triggering and its relevant evidence. It describes one execution's instructions and input, distinct from both the standing Signal definition and the persistent Goal Task it may advance.

**Delegation**:
An external Agent's execution of a prepared Task, tracked by its requesting execution record.

**Delegation Context**:
A Context representing one Delegation, including its external session, progress and outcome.

**Approval Queue**:
The shared collection of pending requests for user decisions or input needed to advance work. Each response returns to the invocation that raised the request; permission requests and requests for missing information remain distinct.
