# Route Context changes through their actors

Agents submit Context Commands such as triggering a Signal or adding a Message to a Signal Run Context through actor mailboxes; they do not write Context storage directly. Effect mailbox requests are delivery and processing instructions, not entries in the Context's domain history. Each actor serializes commands and persists the resulting zero or more Messages. This preserves ordered, recoverable Context histories and avoids concurrent writers bypassing the actor's decisions, at the cost of requiring separate command-delivery and domain-state persistence semantics.

The shape and routing of messages after Signal evaluation are not decided here.
