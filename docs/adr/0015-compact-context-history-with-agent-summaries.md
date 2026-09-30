# Compact Context history with authoritative Agent summaries

Context state consists of an authoritative Summary Checkpoint plus the ordered Messages appended after its cutoff. A background Agent may summarize a committed Message prefix; once the resulting checkpoint is durably installed, the covered Messages may be deleted. This bounds both Agent context size and local storage in service of Signal detection rather than source-data archiving, accepting irreversible information loss and making the Agent-produced summary part of the durable source of truth. This supersedes ADR-0011's requirement to retain all immutable Messages.

The summary is installed automatically without human review. The current design retains the checkpoint content and covered-message cutoff, but does not retain the summarizing model or prompt version.

Compaction applies only to Message history. Exact structured state, including mounted Channel configuration and Signal definitions, is stored separately and is never reconstructed from an Agent summary. Configuration changes may emit audit Messages, but those Messages are not the authoritative configuration.
