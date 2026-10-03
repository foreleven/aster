# Make Goal Input handoffs visible and reconcilable

An accepted Goal Input immediately creates a visible Evaluation Group whose handoff can be pending, running, failed, or awaiting reconciliation. Stable request identities are reconciled before resubmission, and a user Retry creates a new group linked by `retryOf` instead of overwriting the original. This preserves accepted work and makes Pi availability and uncertain submission outcomes inspectable without confusing planning status with external execution status.
