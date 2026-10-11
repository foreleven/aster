# One durable business state authority per entity

Status: accepted; implementation pending the core design interview.

Signal currently reconstructs authoritative state from AgentConversations events while publishing a DurableContext projection; TaskExecution maintains another business checkpoint journal. Each domain entity will instead own one DurableContext containing its business state, receipts, pending deliveries and execution checkpoints, eliminating replay/projection authorities and mirrored committed Refs. AgentConversations retains native sessions and input/output evidence: its separate admission boundary still requires handoff reconciliation, and external uncertain-outcome markers remain mandatory.
