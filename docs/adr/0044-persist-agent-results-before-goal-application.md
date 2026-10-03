# Persist Agent results before applying Goal state

Pi persists each Agent Run's transcript and structured Evaluation Result before notifying GoalActor. GoalActor validates the stable Evaluation Group identity and is the only writer that applies Goal Summary, Task, Signal association, and Timeline state; recovery replays a persisted result instead of invoking the model again. This separates durable model execution from Goal business state while preserving replay after a callback or process crash.
