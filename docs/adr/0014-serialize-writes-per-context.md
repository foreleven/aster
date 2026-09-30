# Serialize writes within each Context

A `ContextEntity` processes at most one state-changing Command at a time, while different Context entities may process writes concurrently. Each handler observes the state produced by all earlier committed Commands and appends Messages in a deterministic per-Context order. This gives every Context a single-writer boundary, trading same-Context write throughput for simpler invariants and reliable event ordering.
