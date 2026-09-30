# Gate Signal evaluation with a fast decision

Context updates do not automatically run an agent to evaluate user-defined Signals. A fast typed Jev decision returns the IDs of Signals worth deeper evaluation; Jev does not perform that evaluation itself. The first local slice judges every configured Signal and does not use BM25. Retrieval may be added if the Signal set or Jev cost grows; adding it earlier would create another false-negative path. The Jev gate reduces unnecessary agent runs compared with evaluating every update. Automatic detection is best-effort: skipped updates remain in their Contexts, later updates may be reconsidered, and users can explicitly force an evaluation; false negatives are not ruled out.

Signals do not declare a static list of input Channels. Jev selects which Signals warrant evaluation for Context changes across Channels.

One eligible Context update invokes at most one deep evaluation of its candidate Signal set. That evaluation may trigger zero or multiple Signals and may inspect other Contexts by path.

Signal Run Context updates, including delegation results, do not enter the gate for triggering other Signals. Signal-to-Signal chaining is outside this design; construction and delivery of messages within a run remain to be designed.

Newly created Signals only observe subsequent Context changes by default. Evaluating earlier stored Contexts requires an explicit user-requested backfill, avoiding surprise bulk delegation of old work.
