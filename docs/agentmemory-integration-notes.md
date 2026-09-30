# agentmemory integration findings

Reviewed on 2026-09-28 against the published `@agentmemory/agentmemory@0.9.29` package, the upstream README, and the current upstream PostToolUse and configuration sources. The application uses upstream observation processing with capture eligibility based on its own domain model; the first slice's session mapping is recorded below.

## Recommended usage

The upstream integration combines automatic observation capture with explicit memory writes by an Agent. Lifecycle hooks capture prompts, tool inputs and outputs, failures, and session activity. Agents use `memory_save` when a durable fact or decision becomes clear, and search memory before beginning related work. The `memory-discipline` skill recommends saving decisions with their reasons, preserving environment facts and non-obvious constraints, and recording user corrections as lessons.

## Automatic capture

The shared PostToolUse script sends a structured observation to `POST /agentmemory/observe`, including `sessionId`, `project`, `cwd`, `timestamp`, `hookType`, and tool input/output data. It truncates tool output to 8,000 characters and makes a bounded, best-effort request.

The observation handler performs deduplication and privacy filtering before storage and indexing. Its default synthetic compression derives a title, a short narrative, file references, and fixed importance/confidence values from the observation. It does not extract semantic facts or concepts with an LLM. BM25 retrieval works without an LLM or embedding provider.

LLM-written per-observation compression requires both a configured LLM provider and `AGENTMEMORY_AUTO_COMPRESS=true`. Session summaries require an LLM provider; the summarize handler explicitly skips them when the provider is `noop`. This requirement is separate from the per-observation compression flag. Graph extraction has its own opt-in flag. Consolidation is enabled by default when a provider is configured and can be disabled separately.

## Agent adapters

| Agent       | Capture mechanism                                                                                                 | Lifecycle and recall                                                                                                                                                                               |
| ----------- | ----------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Claude Code | Plugin lifecycle hooks; PostToolUse sends observations                                                            | SessionStart registers the session; Stop/SessionEnd send session end. Automatic context injection is controlled by `AGENTMEMORY_INJECT_CONTEXT`. MCP and skills support search and explicit saves. |
| Codex       | Published plugin hook manifest with SessionStart, UserPromptSubmit, PreToolUse, PostToolUse, PreCompact, and Stop | Reuses the shared hook scripts and exposes memory tools through MCP. Availability depends on the host's plugin/hook support.                                                                       |
| Cursor      | Cursor hook manifest with session, prompt, tool, failure, and stop events                                         | Reuses shared scripts with Cursor-compatible payload handling.                                                                                                                                     |
| OpenCode    | Native plugin events for prompts, messages, tools, and session activity                                           | Idle status triggers summarize; session deletion ends the session and requests consolidation. Its system transform injects memory context and tool instructions.                                   |
| pi          | Native extension events such as input, tool_result, and agent_end                                                 | Performs prompt-related recall before the Agent runs, registers memory tools directly, and ends the session on quit.                                                                               |

These findings come from adapter manifests and source inspection, not end-to-end verification in each Agent host.

## Implication for the Context design

The application uses the common Context change mechanism to coordinate domain processing. For source activity, normalized observations are submitted by the global Memory Actor after the extraction Agent confirms an actual Signal triggering and its Signal Run is initialized. Ordinary source Message changes and Jev candidates do not independently write memory. Initialization capture of discovered account and mailbox identity facts remains part of the design. agentmemory owns configured compression, indexing, and consolidation, and Agents can save settled facts and decisions through its memory tools.

The upstream adapters broadly capture lifecycle events; the application deliberately ties activity capture to its Signal lifecycle. Each Signal Run corresponds to one upstream memory session. Its memory input is a triggering-time snapshot of the run and its source Contexts, including public `path`, fixed `description`, `state`, and `messages`. Private actor bookkeeping and runtime resources remain internal. In the print-only slice the session ends after input processing, requesting a summary if an LLM provider is available. Initialization profile captures use separate short sessions per source Context with the same completion rule. The adapter maps each public snapshot to a `post_tool_use` observation with the Context path as tool input and the snapshot as tool output; lifecycle design for real delegated execution is deferred.

The recall interface follows upstream's progressive disclosure. `aster memory search "<query>" [--limit N]` returns the compact smart-search envelope, defaulting to 10 candidates with IDs, titles, types, scores, timestamps, and available source Context paths. It performs no automatic expansion. The Signal extraction Agent then calls `aster memory expand <obs-id> [<obs-id>...] [--session ID]` only for relevant candidates, receiving the expanded envelope with full details and provenance. Expansion follows upstream's 20-record cap and reports truncation; missing observations are omitted. The pinned upstream cannot expand consolidated memory hits through smart-search, so the adapter resolves only selected consolidated IDs through the memories endpoint and their source observation references. The adapter uses agentmemory's retrieval capabilities under the configured embedding mode.

The upstream `recall` skill permits the user's original wording as the query and defaults to 10 candidates. The MCP tool describes progressive disclosure through `expandIds`; the pi adapter additionally formats at most five hits for its Agent. Our CLI follows the MCP compact/expanded pattern. The Agent reads expanded details before relying on facts, and tries alternative search terms when needed without inventing memories. Retrieval does not itself merge or delete stored records.

## LLM provider configuration

Upstream reads `~/.agentmemory/.env` and the worker's process environment; process environment variables override file values. This allows a managed application to supply provider settings when starting its memory subprocess. Changes require restarting that subprocess.

Provider selection is detected from available keys. In 0.9.29 the priority is OpenAI, MiniMax, Anthropic, Gemini, then OpenRouter. `OPENAI_API_KEY_FOR_LLM=false` excludes OpenAI from LLM selection while leaving its key available for embedding use. With no detected provider, the default is `noop`; `AGENTMEMORY_ALLOW_AGENT_SDK=true` explicitly enables the Claude Agent SDK fallback.

| Provider                            | Key                                  | Model setting      | Endpoint setting                                            |
| ----------------------------------- | ------------------------------------ | ------------------ | ----------------------------------------------------------- |
| OpenAI or OpenAI-compatible service | `OPENAI_API_KEY`                     | `OPENAI_MODEL`     | `OPENAI_BASE_URL`; Azure also supports `OPENAI_API_VERSION` |
| MiniMax                             | `MINIMAX_API_KEY`                    | `MINIMAX_MODEL`    | `MINIMAX_BASE_URL`                                          |
| Anthropic                           | `ANTHROPIC_API_KEY`                  | `ANTHROPIC_MODEL`  | `ANTHROPIC_BASE_URL`                                        |
| Gemini                              | `GEMINI_API_KEY` or `GOOGLE_API_KEY` | `GEMINI_MODEL`     | Built-in Gemini endpoint                                    |
| OpenRouter                          | `OPENROUTER_API_KEY`                 | `OPENROUTER_MODEL` | Built-in OpenRouter endpoint                                |

The OpenAI-compatible provider calls chat completions and supports custom hosted endpoints, Ollama, LM Studio, vLLM, llama.cpp, and Azure. A System One decision endpoint alone does not satisfy this interface: the Laya investigation so far verified `/v1/systemone`, and did not verify `/v1/chat/completions`.

`MAX_TOKENS` controls the output token limit, and `AGENTMEMORY_LLM_TIMEOUT_MS` controls raw-fetch provider timeouts. Per-observation LLM compression additionally requires `AGENTMEMORY_AUTO_COMPRESS=true`. Embedding configuration is separate; OpenAI and Gemini keys also participate in automatic embedding-provider detection.

### MiniMax China service

MiniMax's current China documentation lists `https://api.minimax.cn/anthropic` as the Anthropic-compatible base URL. The published provider appends `/v1/messages`, supplies `x-api-key` and `anthropic-version: 2023-06-01`, and supports the configured `MiniMax-M3` model. Set `/memory.config.llm` to `provider: minimax`, `model: MiniMax-M3`, the China `baseUrl`, and an `apiKeyEnv` naming the credential in the project's `.env`. The adapter maps that named credential to the upstream `MINIMAX_API_KEY`; the original variable can have a custom name. `autoCompress: true` enables observation compression, and completed sessions also request summaries.

Verified against the China service: HTTP 200 for `MiniMax-M3`, real account/mailbox observation compression, a completed mailbox session summary, and CLI recall of the newly compressed mailbox observation with `/lark/mail` provenance. Embeddings remain disabled, so recall uses BM25. A model may summarize Chinese source content in English; search terms matching the stored narrative or email address retrieve that content.

## Sources

- [README: Memory Pipeline](https://github.com/rohitg00/agentmemory#how-it-works)
- [README: LLM providers and defaults](https://github.com/rohitg00/agentmemory#configuration)
- [PostToolUse hook](https://github.com/rohitg00/agentmemory/blob/main/src/hooks/post-tool-use.ts)
- [Memory discipline skill](https://github.com/rohitg00/agentmemory/blob/main/plugin/skills/memory-discipline/SKILL.md)
- [Recall skill](https://github.com/rohitg00/agentmemory/blob/main/plugin/skills/recall/SKILL.md)
- [Smart search and expansion](https://github.com/rohitg00/agentmemory/blob/main/src/functions/smart-search.ts)
- [Codex hook manifest](https://github.com/rohitg00/agentmemory/blob/main/plugin/hooks/hooks.codex.json)
- [OpenCode adapter](https://github.com/rohitg00/agentmemory/blob/main/plugin/opencode/agentmemory-capture.ts)
- [pi adapter](https://github.com/rohitg00/agentmemory/blob/main/integrations/pi/index.ts)
- [Provider selection and configuration](https://github.com/rohitg00/agentmemory/blob/main/src/config.ts)
- [OpenAI-compatible provider](https://github.com/rohitg00/agentmemory/blob/main/src/providers/openai.ts)
- [MiniMax China Anthropic-compatible API](https://platform.minimaxi.com/docs/api-reference/text-anthropic-api)

## Published runtime findings and verification

The project pins npm package `0.9.29`, whose CLI and SDK pin iii engine `0.11.2`. Current main-branch installation documentation may describe a newer engine; the managed runtime follows the published package.

The CLI has global worker/engine PID files even when ports and data paths differ. It does not rewrite hardcoded template ports from `--port`, and its first-install branch bypasses the rewritten runtime data template. The adapter generates a complete application-owned template before invoking the CLI, explicitly configuring REST, streams, worker manager, and data directories. It removes the template’s exec worker because the CLI already starts the memory worker.

The engine’s file-based KV adapter saves periodically (default five seconds) and does not flush from its destroy method. The managed template selects 100 ms saves, and graceful shutdown allows several ticks after worker index saving. Health routes are available before index loading; startup also waits for viewer readiness before exposing recall.

Verified locally: zero-LLM capture, BM25 search, source Context provenance, recall after a full worker/engine restart, and closing all owned listening ports. MiniMax China LLM compression and summaries are also verified with the configured credential and model. Other remote LLM and embedding backends require their actual keys/models; adapter environment selection and asynchronous compression/session ordering are covered by tests.

The Codex CLI's default read-only sandbox blocks local REST queries. The application uses a named profile extending `:read-only`, the managed network proxy, and an explicit `127.0.0.1` allow rule for memory access. It denies the project `.env` file and strips project service keys from the subprocess environment. CLI provenance queries use a read-only SQLite connection. Profile settings are passed to each invocation and do not modify user configuration; see the [official Codex configuration reference](https://developers.openai.com/codex/config-reference/).

Verified with Codex CLI `0.156.1`: dynamic identity-only description generation, and an actual extraction-Agent invocation of CLI memory search returning source paths. The network proxy must use `full` method mode because smart-search uses POST; the domain allowlist still restricts sandboxed command traffic to `127.0.0.1`. Its `limited` mode rejected this query. Quoted file paths and dotted IP literals are supplied in a single TOML table because dotted CLI overrides otherwise split IP components.
