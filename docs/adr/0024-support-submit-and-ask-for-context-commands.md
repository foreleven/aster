---
status: superseded by ADR-0026; interaction API to be redesigned
---

# Support submit and ask for Context Commands

Typed actor references expose two delivery modes for durable write Commands. `submit` persists the Command and returns its command ID immediately; `ask` performs the same submission and waits for the durable completion associated with that ID, decoding the Command's Schema-defined success or typed error. Exact state changes, appended Messages, and encoded completion commit in one SQLite transaction. An in-process `Deferred` may wake current waiters, but SQLite remains authoritative and a waiter can reattach by command ID after actor reactivation.
