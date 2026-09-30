# Keep Channel credentials out of agent-readable Context

Agents may inspect Contexts by path, including Contexts that carry Channels. Credentials needed by a Channel must not appear in Context content that an agent can read. The Channel may use its credentials, but mere access to its Context must not disclose them. How credentials are protected and supplied is left to implementation design.
