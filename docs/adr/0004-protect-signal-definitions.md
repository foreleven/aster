# Keep Signal definitions under user control during evaluation

In the initial design, users define Signals with separate trigger-condition and delegated-task descriptions, plus target agents and confirmation requirements. The evaluating agent may decide that a Signal has triggered and create its Signal Run Context, but cannot change the reusable definition, redirect its work, or change its confirmation requirement. This limits automation in exchange for keeping externally influenced Contexts from silently changing user intent; automatic Signal discovery and optimization are deferred to a later design.

Edits to a Signal definition affect only future triggerings. Existing Signal Run Contexts retain the action, target agent, and confirmation requirement in effect when they were created.

The [Goal design](../goals-design.md#accepted-goal-driven-signal-generation) extends the initial scope to Agent generation and adjustment of Signals for explicitly user-created Goals. The deferral above therefore no longer applies to that case. Goal-derived Signals participate in normal evaluation and use `auto` in the first version. Evaluating a triggering alone still does not authorize editing its definition, and edits do not retroactively change existing runs.
