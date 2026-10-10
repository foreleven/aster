# Remove independent Goal screening audit storage

Status: accepted and implemented; supersedes [ADR 0041](0041-persist-goal-screening-evaluations-separately.md).

Independent calibration samples are no longer a product requirement. Remove FileGoalScreening and GoalScreeningStore, retaining the authoritative durable routing state and Goal inputs plus structured operational logs. The existing dataset has no product reader, duplicates evidence outside the business state, and makes an audit write failure block Goal screening; removing it simplifies both the adapter and core's decision path.

Ordinary logs and routing records do not preserve an equivalent complete calibration dataset. This deliberately gives up future full-sample collection; it does not authorize deleting existing runtime files. Implemented by the infrastructure refactor.
