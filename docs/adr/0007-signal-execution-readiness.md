# Evaluate execution readiness for the whole triggered Signal

Before delegation, Jev makes a typed decision about the entire Signal Run Context and relevant Contexts: executable with a selected Workspace Context, needs user input, or currently not executable. Workspace choice is one part of this decision, not a standalone check; the decision neither changes the Signal's task or target agent nor overrides its confirmation requirement. This enables automatic workspace discovery while preventing uncertain work from starting, at the cost of possible delays from conservative decisions.

When user input is needed, the missing information is requested as a message in the Signal Run Context. The user's reply is another message in that Context and triggers a new readiness decision.

A currently-not-executable decision records its reason in the Signal Run Context but does not permanently close it. New relevant Context or an explicit user retry may lead to another readiness decision.
