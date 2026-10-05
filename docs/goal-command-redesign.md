# Goal command boundary

The current design is [Goal, conversation, Tasks and Signals](goals-design.md).

`SubmitInput`, `End` and `RetryTurn` are the public Goal commands. Each carries an explicit reply address and a stable request identity. The Goal mailbox persists accepted business state and the receipt before replying. Runtime activation/readiness are private controls.

`SubmitInput` accepts user input, Personal messages, screened Context changes, Signal occurrences and Task feedback. Only Context changes pass through the Goal Agent Gate before Pi. Producer ownership and expected revisions are checked before acceptance. Pi request identity is the input identity, with no intervening evaluation entity.

`RetryTurn` accepts only a known failed input without a retry successor. Unknown Pi or external outcomes require reconciliation of the original identity. `End` stops new conversation input, interrupts active reasoning, revokes unstarted Tasks and deactivates Signals. External executions that already started retain their independent owner.

Private messages apply the Agent Gate outcome, assistant response and business progress in the mailbox with generation checks. `start_task` and `set_signal` call shared Task/Signal owners directly. Their durable receipts belong to those owners. Goal no longer accepts `RetrySignalDelivery`, applies plan batches, freezes read catalogues, stores evaluation groups, or interprets structured next steps.

The public Timeline displays each accepted input, delivery status and response. Task and Signal records are read independently. No historical-data compatibility layer is provided.
