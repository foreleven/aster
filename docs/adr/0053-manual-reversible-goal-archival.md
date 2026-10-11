# Manual reversible Goal archival

Status: accepted; implementation pending the core design interview.

A Goal represents a continuing responsibility and never ends or completes. Replace End/completed behavior with user-only, reversible archival: stop new automatic evaluation and owner Signal triggers, retain accepted inputs, feedback and history, and resume on unarchival. Accepted Tasks retain independent execution ownership and require explicit cancellation; archival does not establish cancellation of external work.

Unarchival skips scheduled times missed during archival: cron calculates its next time from restoration and expired once rules become exhausted. Already-admitted occurrences remain retained. Owner archival suppresses eligibility without changing independently paused or deleted Signal definitions.
