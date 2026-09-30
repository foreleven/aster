# Require an explicit Signal action for external writeback

Delegation results are recorded as messages in the local Signal Run Context by default. The system does not automatically reply in chat, update tickets, or act on alerts; external writeback requires an action explicitly defined in the Signal and is performed through the relevant Channel. This sacrifices implicit convenience to keep side effects under user control.
