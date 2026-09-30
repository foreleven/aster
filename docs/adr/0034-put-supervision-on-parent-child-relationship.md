# Put supervision on the parent-child relationship

The parent selects a child's `restart`, `stop`, or `escalate` directive when spawning it. The default is one-for-one restart, limited to five restarts within one minute with exponential backoff from 100 ms to 10 s and 20 percent jitter; an exhausted budget becomes `stop`. This amends ADR-0022's consecutive-attempt limit and child-type override so supervision remains owned by the parent relationship, as in Akka Typed.
