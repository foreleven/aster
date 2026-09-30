# Aster

Aster is a local Personal Agent application that observes your work context, remembers relevant information, follows Goals, and delegates tasks to external agents. This pnpm monorepo includes its Effect 4 RC Actor runtime and integrations. [docs/actor-design.md](docs/actor-design.md) defines the Actor contract; [docs/core-design.md](docs/core-design.md) describes the wider Context and Signal design.

## Workspace

Runtime ownership, integration installation and Effect configuration are specified in [docs/runtime-design.md](docs/runtime-design.md).

- `packages/agent` — thin Effect wrapper around pi with named models and isolated runs.
- `packages/actor` — the Effect 4 RC Actor runtime, SQLite persistence, and tests.
- `packages/core` — configuration, Contexts, Signals, Goals, decision processing, and durable domain state.
- `packages/lark-integration` — Lark account, mail and user-identity IM polling actors.
- `packages/memory` — managed agentmemory, capture/recall, and durable Context provenance.
- `packages/integrations` — YAML/filesystem, System One transport, Codex/Doubao adapters, and Lark/memory assembly.
- `apps/local` — CLI, startup composition, process lifecycle, and the local HTTP/SSE interface.
- `apps/web` — independently replaceable React/Vite Goal conversation client.

Use Node 24 or later. Run `pnpm install`, `pnpm typecheck`, and `pnpm test` from the repository root. For local configuration and startup, see [apps/local/README.md](apps/local/README.md).

## Code quality

Coding agents must follow [AGENTS.md](AGENTS.md): implementations prioritize Effect and use the pinned upstream source under [repos/effect](repos/effect) as read-only reference. [agent-patterns/effect.md](agent-patterns/effect.md) captures project conventions; [repos/README.md](repos/README.md) records source provenance and maintenance.

ESLint 10 uses the root flat configuration for all JavaScript and TypeScript sources and tests, including React Hooks and Fast Refresh checks. Prettier 3 formats maintained source, configuration, and documentation. Generated output, dependencies, local credentials, runtime data, and the package-manager-owned lockfile are excluded from formatting.

- `pnpm lint` — check code with zero warnings allowed.
- `pnpm lint:fix` — apply ESLint's automatic fixes.
- `pnpm format` — format the entire repository.
- `pnpm format:check` — verify formatting without writing files.
- `pnpm check` — run lint, formatting checks, and TypeScript checks.
- `pnpm test` — build all packages and run backend tests.
- `pnpm test:web` — build all packages and run browser tests.

On a fresh checkout, run `pnpm build` before `pnpm check`: workspace packages resolve one another's generated declarations. Intentionally unused parameters use an `_` prefix. Explicit `any` remains allowed for erased Effect Actor protocols and external SDK boundaries; strict TypeScript checking still applies. Keep rule exceptions local and explain why they are needed.

## Effect language service in Zed

`@effect/language-service` is installed at the workspace root and enabled in every backend package's `tsconfig.json` (including inherited build/test configs). It supports the project's Effect 4 APIs. `.zed/settings.json` configures Zed's default `vtsls` server to use the workspace TypeScript installation so the plugin can load. After installing dependencies, reopen the project in Zed or run **editor: restart language server**.

The plugin provides Effect diagnostics, hover information, completions, and refactors. To inspect diagnostics outside the editor, run `pnpm exec effect-language-service diagnostics --project apps/local/tsconfig.json`. The optional TypeScript compiler patch is not enabled, so ordinary builds retain their existing diagnostics. The VS Code/Cursor extension and its runtime Tracer panel are not part of this Zed setup.
