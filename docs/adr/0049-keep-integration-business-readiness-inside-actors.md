# Keep integration business readiness inside Actors

Accepted during the Lark reference-integration review on 2026-10-10; implemented by the reference rewrite. Runtime owns activation and framework lifecycle, while remote retrieval, catch-up, connection status and business retry policy belong to the owning Actors. Remove the shared integration business-ready handshake instead of having Runtime wait for source catch-up or observe Context state.

This amends the readiness portion of ADR-0040. A successfully initialized Actor may still lack remote data; its query contracts must report that business condition explicitly. Configuration/acquisition failures and Actor defects retain their existing framework failure semantics. The trade-off is that runtime readiness no longer promises fresh external evidence, in exchange for keeping domain progress and failure policy out of orchestration.
