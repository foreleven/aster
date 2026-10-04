# Business integrations

`@aster/integrations` connects Aster to external business systems. It contains `lark/` for Lark accounts, IM, and mail, and `mail/` for generic mail retrieval. See [Lark](src/lark/README.md) for its Actor, readiness, publication, and recovery contracts.

Integrations depend on core contracts and the existing Actor/Agent packages. They receive model and decision services through Effect dependencies, never by importing `@aster/infra`. The local host selects integration Layers; `AsterRuntime` owns their activation, readiness, and shutdown. This package does not re-export infrastructure, memory backends, or Models.

The generic mail adapter is exported from `mail/`. `MailIntegration.layer` provides the configured `MailSettings` and `MailFetcher` services; `MailSettings` reads `contexts./mail.config.mailboxes` from the captured `ConfigProvider`, while `mailFetcherLayer(settings.mailboxes)` is available for standalone composition. Passwords are decoded into `Redacted` values. Each mailbox may set `protocol: "imap" | "pop3"`, `host`, `port`, `secure`, `username`, `password`, `folder`, and `maxMessages`; IMAP and POP3 are supported because SMTP is a submission protocol.

Transport and protocol tests live here. Cross-integration, source-to-Memory, and application lifecycle tests remain in `apps/local/test`. Tests use fake transports.
