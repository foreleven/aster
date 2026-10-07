# Require an explicit Signal action for external writeback

Status: Superseded. The dedicated Task-result publication chain has been removed. Tasks return results to Goals; any future external send belongs to an explicit command on the destination Context, with authorization and delivery handling owned by that integration. The current Lark Chat Context does not expose a send command.

Delegation results are recorded as messages in the local Signal Run Context by default. The system does not automatically reply in chat, update tickets, or act on alerts; external writeback requires an action explicitly defined in the Signal and is performed through the relevant Channel. This sacrifices implicit convenience to keep side effects under user control.
