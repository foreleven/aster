# Aster Context workspace

React 19, Vite, Tailwind CSS 4 and Radix primitives. The workspace presents a Context tree, the selected conversation or record, and related work. `/goals/personal` is the default assistant and uses the same Goal UI as every other Goal.

Run `pnpm start` at the repository root and open the printed loopback URL (default `http://127.0.0.1:4317`). Local start serves Vite source. To develop separately, run `pnpm --filter @aster/web dev` with the backend running.

## Goal conversations

The Timeline shows actual user messages and selected assistant replies from Pi. Context evidence, tool calls/results and execution envelopes do not appear as chat messages. Notes filters user messages from the same feed. Earlier pages use stable Pi entry cursors; invalidation refreshes loaded pages and reconnect refreshes mounted queries.

The composer sends instructions through the public Goal API. Failed sends retain their text. Known failed conversation inputs expose explicit retry; uncertain retry admission retains the original identity and payload across navigation and reconnect within the app instance. Browser-only pending identities do not survive full document reload.

Simple exchanges stay in the main conversation. Related Tasks show sustained work, including internal Agent execution and external delegation. Typed Task details include instructions, follow-ups, results, available tools, sources and approval requests. Failed or uncertain execution supports explicit resumption.

Goal End requires confirmation, marks the Goal complete and stops future owned Signal triggering. Submitted work may still finish. Completed Goals remain selectable. Edit, Pause and Archive remain disabled because the API does not implement them; Goal creation uses configuration.

## Contexts and decisions

The tree uses real Context paths and revisions and never invents parents. Selection uses the `context` URL query parameter and survives reload/back/forward. Missing records display an unavailable state. Restricted projections retain path and description but omit contents and state inspection.

Generic Contexts expose public summaries, messages and related records. Approvals use typed confirmation, permission and question forms. Queue records remain the authority for human decisions; conversational replies explain the request. The UI performs no domain processing and imports no Actor or persistence internals.

On narrow screens the navigation becomes a drawer and related work follows the conversation. Dates use the browser's local timezone.

## Application API

The root Atom registry shares ApplicationClient via AtomRpc. `/api/rpc` serves typed queries/mutations; `/api/events` delivers committed invalidation keys into Reactivity. EventSource lifetime is scoped to subscribers. Query loading/errors use AsyncResult; refresh can recover a failed connection. Display schemas keep arbitrary private state out of rendering logic.

Feature components live in `src/contexts` and `src/goals`; query adapters, display projections and the inspector live in `src/api` and `src/dashboard`.

## Validation

`pnpm test:web` builds the workspace and runs Playwright against isolated fixture transports and a real local HTTP/SSE server with fake domain services. It never invokes real models or integrations. Install Chromium with `pnpm --dir apps/web exec playwright install chromium`, or use installed Chrome with `PLAYWRIGHT_CHANNEL=chrome pnpm test:web`.

Tests cover desktop/mobile navigation, natural messages, pagination, Task details, approval/resumption controls, retained uncertain request identities, SSE reconnect and malformed projections. `test/fixtures.js` is test-only visual data.
