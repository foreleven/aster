# Isolate Goal Agent sessions and hand off persisted inputs

Each Goal owns one durable Pi Session with one primary Conversation and multiple Agent Runs. GoalActor remains the single writer for Goal business state and first persists every Goal Input, then submits an idempotent durable handoff to the Pi Conversation; Pi owns only the native Agent transcript and model/tool execution. This preserves Goal ordering and recovery when the two stores cannot commit atomically, at the cost of representing a temporarily pending or failed handoff.
