# Aster Context workspace

React 19, Vite, Tailwind CSS 4, and Radix primitives. The application is a three-column Context workspace: an expandable tree of existing Context paths, the selected Context and its messages, and related work. Personal Agent is the default when `/personal` exists; existing Goal workspaces retain their timeline and action panels. Home, Search, Library, and Settings are outside this implementation.

Run `pnpm start` at the repository root. Local start serves this frontend from source through Vite, so changes do not require a workspace build. Open the local URL printed by Aster (default `http://127.0.0.1:4317`). For frontend development on a separate port, run `pnpm --filter @aster/web dev` with the backend running.

## Contexts and Personal Agent

The navigation tree uses real public Context paths, with the nearest available ancestor, search, and actual revisions. It never invents missing parent records. Selection uses the `context` URL query parameter and survives reload, back/forward, and SSE reconnect. If the selected record disappears, the UI reports it as unavailable instead of choosing a different record.

Personal messages are decoded with the shared envelope and ordered by sequence. The composer submits `SendPersonalMessage` with an expected revision and a stable business request ID. Unknown submission outcomes retain the frozen message and request identity across Context navigation and reconnect; an explicit retry sends that same payload. A definite conflict refreshes state and permits a new explicit submission. Pending browser request identities currently last for the app instance, not a full document reload.

The right panel shows persisted run attempts and delivery outcomes. Only the latest failed head input can be retried. An unknown internal Goal delivery can be reconciled using its original operation ID and payload. Delivery receipt revision is distinct from source acceptance. The UI does not infer model availability, automatically retry failed work, or perform domain processing.

Generic Contexts show their summary, business messages, public state, and linked records. Approvals retain the existing typed response forms. On narrow screens the tree becomes a drawer and related work follows the conversation.

## Goals

- Goals remain selectable in the Context tree after completion and retain their history. Business status is independent of Actor availability.
- Timeline shows native Goal history, newest first, grouped by date. It supports event filtering, earlier pages, and expandable context references. Internal system, tool, and native Pi frames are excluded from message presentation. Notes shows user messages from the same history; it is not a separate note store.
- Related opens linked public records in the existing inspector. Details shows progress, completion criteria, and access to stored state.
- The composer sends a note or instruction with Enter; Shift+Enter inserts a newline. The paperclip inserts a reference to an available Context. Failed sends retain their text and are not automatically retried.
- Task and Signal titles open their records. Pending approvals for this Goal's executions retain the existing confirmation and input forms.
- Goal actions can end a Goal after explicit confirmation. Ending marks it complete and deactivates its generated Signals; already submitted work may still finish.
- Edit, Pause, and Archive are disabled because the public application API does not implement those operations. Creation still uses Aster configuration. The UI does not invent local-only lifecycle changes.
- On smaller screens, tasks and Signals follow the conversation. Mobile navigation opens from the breadcrumb bar. Dates are formatted in the browser's local time zone; absent creation times, personal categories, external account identity, and attachment previews are not fabricated.

Feature components and scoped styling live in `src/contexts` and `src/goals`. Existing inspector, approval forms, API adapters, and display projections live in `src/dashboard` and `src/api`.

## Reactive application API

The root Atom registry shares an `ApplicationClient` AtomRpc runtime. Typed queries and mutations use `/api/rpc`; `/api/events` carries committed query invalidation keys into the same runtime's Reactivity service. Query errors and loading state come from AsyncResult. The scoped EventSource closes when its connection atom is released; reconnection refreshes all queries. The refresh button can recover a failed connection. The Goals workspace does not start a periodic runtime telemetry poll.

Display Schemas decode the fields the UI presents and keep arbitrary Context fields out of rendering logic, while retaining raw state for inspection. History caches loaded pages and fetches only missing older pages or new tail entries after invalidation. Runtime state and business status remain separate in the inspector.

See [Reactive application API](../../docs/reactive-api-design.md) for contracts, externally supplied Layers, and history pagination.

## Validation

`pnpm test:web` builds the workspace and runs Playwright tests against isolated fixture transports and a real local HTTP/SSE server with fake domain services. It never starts real integrations or paid models. Install the test browser with `pnpm --dir apps/web exec playwright install chromium` if needed. To use an already installed Chrome instead, run `PLAYWRIGHT_CHANNEL=chrome pnpm test:web`.

Tests cover desktop/mobile layout, navigation, filters and tabs, message submission/failure, completion confirmation, context inspection, scoped approvals, history pagination, query invalidation, connection recovery, and malformed data. `test/fixtures.js` contains a reference-shaped Hokkaido dataset for visual QA; production code never imports it.

Personal delivery cards cover Goal messages and Signal create/update commands. Signal cards show the proposed task, timing, activation and per-Run confirmation requirement; all cards show durable attempt counts and receiving Context revision. Unknown outcomes reconcile using the original request ID, payload and target revision. Signal authoring currently uses Personal replies or the typed API; there is no separate Signal editor.

Approval controls use Personal command admission when Personal is present. The queue owner validates the decision and target revision before committing a receipt. Pending decisions retain their original payload and request ID across navigation and SSE reconnect within the app instance; ambiguous admission offers an explicit reconciliation action. A durably queued decision links to its Personal delivery card. Confirmed domain rejection permits a new explicit response after refreshed state. Legacy dashboards without Personal retain their original approval endpoint. Full document reload persistence for browser-only uncertain admission is not implemented.

Delegation paths render the typed `InspectPersonalDelegation` projection: executor, state, original instructions, result/error, sources, and approval/input requests. This view replaces generic state/native-message rendering for Delegations. It observes both path-specific invalidation and reconnect-wide refresh, and links to the owning Run and approval queue. An absent execution handle remains explicitly uncertain.

Personal one-time Task delivery cards reconcile the original request identity and link to the admitted Run. Run workspaces decode the shared prepared Task schema, display instructions and attributed input, readiness/confirmation stages and the recorded outcome. Dashboard Run counts include independent Personal, Goal and Signal Runs. Browser coverage includes lost-acknowledgement reconciliation and a reconnect from awaiting confirmation to completion.

Failed or uncertain Run workspaces offer explicit execution resumption. The original command identity and both revisions survive navigation and reconnect while admission is uncertain. Run handoff errors remain visible, and Delegation inspection shows its business resumption state without exposing provider handles. Tests cover same-payload reconciliation after source revisions change.

Personal approval-request cards show the source Context/revision, support reconciliation with the original demand identity, and link to the pending queue entry. Requesting approval never sends an approval response. Browser coverage verifies exact-payload reconciliation and navigation without deciding the pending Task.

Context read replies may carry `projection.visibility: restricted`. The tree retains their real path, description and revision; the workspace explains that contents are unavailable and omits the state inspector. Public history pagination uses backend sequence cursors even when private native entries were filtered from a page.

Run workspaces render the typed external publication record separately from local Task completion: exact destination, identity, content, approval state, receipt and error. Pending publication links to Approvals. Unknown delivery explicitly disables automatic resend. Reconnection reloads the current projection, including publication updates to already completed Runs.
