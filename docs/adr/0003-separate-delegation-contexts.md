# Keep Signal Run Contexts separate from Delegations

A Signal is a reusable definition Context. Each triggering creates an independent Signal Run Context whose ordered messages include confirmation and execution results; each execution attempt also has its own persistent Delegation Context linked to that triggering. The Signal Run Context records the conversation and decisions about this occurrence, while the Delegation Context tracks that particular execution. These are complementary actor Contexts, not competing places to store the same message history.

Failures are reported back to the Signal Run Context rather than retried automatically. A user-requested retry creates a new Delegation Context, avoiding accidental repetition of side-effecting work.
