# Agent Assembly and Runtime

Status: proposed; superseded by [Agent architecture refactor](agent-refactor-design.md).

Agent behavior is owned by an Assembly, while pi execution is owned by Core's `agent/runtime`. Goal, Task and Screening Assemblies colocate their Markdown instructions, extensions, tools, input shaping and result contracts. Lark summary remains an integration-owned Assembly.

Infra supplies pi-ai's native Models, provider catalog and authentication. The application alias selects a native model; Core does not expose a second ResolvedModel, stream or API-key facade.

Goal and internal Task Assemblies use the thin pi-durable session wrapper. Screening and Lark summary use ordinary pi-agent-core runs. Pi owns native checkpoint, replay and recovery behavior; Goal and Task retain only their business receipts and state.
