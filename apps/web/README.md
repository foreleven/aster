# Aster dashboard

TypeScript dashboard features with React 19 + Vite + Tailwind CSS 4 + shadcn/ui (Radix primitives). UI components live in `src/components/ui`; dashboard features live in `src/dashboard`.

Run `pnpm build` at the repository root, then `pnpm start`. Open the local URL printed by Aster (default `http://127.0.0.1:4317`). Restart an already running older Aster process to enable the new runtime endpoint. For frontend development, run `pnpm --filter @aster/web dev` while the backend is running.

- Overview: live Actor hierarchy, runtime counts, and recent command/supervision events.
- Actors: searchable runtime actors and persisted Contexts; inspect public state, messages, mailbox, pending effects (including timers), command counts, and errors.
- Goals: progress and message flow, user input, explicit end confirmation.
- Signals: definitions, trigger runs, execution stages, source links and delegation/session records.
- Approvals: confirmation, permission and question forms, delivery receipt states and links to execution records.

The root Atom registry shares an `ApplicationClient` AtomRpc runtime. Typed queries and mutations use `/api/rpc`; `/api/events` carries committed query invalidation keys into the same runtime's Reactivity service. Context, approval, history and telemetry reads have separate keys. Mutation errors and loading state come from AsyncResult. The legacy `/api/dashboard` endpoint remains available. Runtime telemetry includes no command payloads, Actor services, or arbitrary spawn metadata. Only runtime metrics are sampled every three seconds; public Context changes refresh matching query atoms through SSE. The scoped EventSource closes when its connection atom is released. Reconnection refreshes all queries. Recent runtime events are limited to 200 in-memory entries for this process. Persistent business messages remain in Context storage; runtime event history is not a durable audit log. A waiting mailbox can coexist with pending asynchronous effects or an executing external Agent. The UI displays runtime and business status separately.

`pnpm test:web` builds the workspace and runs Playwright desktop/mobile interaction tests against isolated fixtures and a real local HTTP/SSE server. First install the test browser with `pnpm --dir apps/web exec playwright install chromium`. Test data is never used by the production app. Actor telemetry and HTTP origin enforcement are separately covered by backend tests.

See [Reactive application API](../../docs/reactive-api-design.md) for contracts, externally supplied Layers and history pagination.

The strict typecheck includes App, dashboard components, query adapters and display projections. JavaScript UI primitives expose React prop types through JSDoc. Display Schemas keep arbitrary Context fields out of rendering logic while retaining raw state for inspection. Derived atoms own Context/message projections and indexes, independently of runtime telemetry. History caches already loaded pages and only fetches missing older pages or the new tail after invalidation.
