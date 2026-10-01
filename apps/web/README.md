# Aster Goals workspace

React 19, Vite, Tailwind CSS 4, and Radix primitives. The default application is a three-column Goals workspace: grouped goal navigation, goal details and conversation, and related tasks, executions, and Signals. Home, Search, Library, and Settings are outside this implementation.

Run `pnpm build` at the repository root, then `pnpm start`. Open the local URL printed by Aster (default `http://127.0.0.1:4317`). For frontend development, run `pnpm --filter @aster/web dev` with the backend running.

## Goals

- Goals are grouped by their persisted business status, independently of Actor availability. Completed Goals appear under Archived and retain their history.
- Timeline shows native Goal history, newest first, grouped by date. It supports event filtering, earlier pages, tool details, and expandable context references. Notes shows user messages from the same history; it is not a separate note store.
- Related opens linked public records in the existing inspector. Details shows progress, completion criteria, and access to stored state.
- The composer sends a note or instruction with Enter; Shift+Enter inserts a newline. The paperclip inserts a reference to an available Context. Failed sends retain their text and are not automatically retried.
- Task and Signal titles open their records. Pending approvals for this Goal's executions retain the existing confirmation and input forms.
- Goal actions can end a Goal after explicit confirmation. Ending marks it complete and deactivates its generated Signals; already submitted work may still finish.
- Edit, Pause, and Archive are disabled because the public application API does not implement those operations. Creation still uses Aster configuration. The UI does not invent local-only lifecycle changes.
- On smaller screens, tasks and Signals follow the conversation. Mobile navigation opens from the breadcrumb bar. Dates are formatted in the browser's local time zone; absent creation times, personal categories, external account identity, and attachment previews are not fabricated.

Feature components and scoped styling live in `src/goals`. Existing inspector, approval forms, API adapters, and display projections live in `src/dashboard` and `src/api`.

## Reactive application API

The root Atom registry shares an `ApplicationClient` AtomRpc runtime. Typed queries and mutations use `/api/rpc`; `/api/events` carries committed query invalidation keys into the same runtime's Reactivity service. Query errors and loading state come from AsyncResult. The scoped EventSource closes when its connection atom is released; reconnection refreshes all queries. The refresh button can recover a failed connection. The Goals workspace does not start a periodic runtime telemetry poll.

Display Schemas decode the fields the UI presents and keep arbitrary Context fields out of rendering logic, while retaining raw state for inspection. History caches loaded pages and fetches only missing older pages or new tail entries after invalidation. Runtime state and business status remain separate in the inspector.

See [Reactive application API](../../docs/reactive-api-design.md) for contracts, externally supplied Layers, and history pagination.

## Validation

`pnpm test:web` builds the workspace and runs Playwright tests against isolated fixture transports and a real local HTTP/SSE server with fake domain services. It never starts real integrations or paid models. Install the test browser with `pnpm --dir apps/web exec playwright install chromium` if needed.

Tests cover desktop/mobile layout, navigation, filters and tabs, message submission/failure, completion confirmation, context inspection, scoped approvals, history pagination, query invalidation, connection recovery, and malformed data. `test/fixtures.js` contains a reference-shaped Hokkaido dataset for visual QA; production code never imports it.
