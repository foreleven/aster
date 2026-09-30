---
status: superseded by ADR-0033
---

# Separate Command failures from actor failures

Expected operational and domain failures are typed Effect errors from a Context Command and do not fail the actor. They are returned or recorded according to the Context Type while the actor continues processing. Only defects, actor-loop failure, or failure to initialize the actor enter the parent's supervision strategy. This prevents ordinary invalid input or downstream unavailability from causing lifecycle churn while reserving supervision for broken actor execution.
