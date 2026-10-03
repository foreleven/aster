# Separate the Goal Timeline from the native Agent transcript

The Goal Timeline is a structured projection of Goal Inputs, Evaluation Groups, Evaluation Results, Task and Signal changes, and linked execution records. The Pi Conversation remains the authoritative native transcript and tool history, but its free-form messages are not parsed to infer Timeline membership. This preserves stable UI and audit objects while allowing model prompts, compaction, and transcript storage to evolve independently.
