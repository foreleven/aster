# Persist Contexts across local program restarts

Contexts are modeled as actors, but their identities, paths, and state must survive restarts of the local program. This preserves the continuity of source history and Signal work, including pending human confirmation; transient in-memory actors alone would lose that continuity, so the system accepts the cost of durable storage and recovery.
