# Aster personal assistant

A conversation-first interface built with React 19, Vite, Tailwind CSS 4 and shadcn/ui (Radix). It opens on the built-in `/goals/personal` Goal. Other Goals appear as spaces. The design uses a warm neutral canvas and self-hosted Geist.

Run `pnpm start` at the repository root and open the printed loopback URL (default `http://127.0.0.1:4317`). Local start serves Vite source. To develop separately, run `pnpm --filter @aster/web dev` with the backend running.

## Conversations and work

- The main conversation displays actual user messages and assistant replies from the public Goal timeline. Context evidence, tool calls and execution envelopes stay out of the conversation.
- Official shadcn Bubble, Message and MessageScroller components render messages, earlier history and scrolling. Suggestions fill an editable draft; they never send automatically. Add context inserts a public Context path into that draft.
- The composer stays available after a message is accepted. It does not wait for model execution or infer a thinking state from the last message. Goal lifecycle and public projection determine whether a conversation is writable.
- Drafts and uncertain submission identities survive navigation within the app instance. Explicit retries reuse the same identity and payload. Full document reload does not preserve browser-only pending identities.
- Activity shows the Goal summary, its authoritative `tasks` list, owned Signals and related approvals. Tasks open the same typed detail page from Activity and the global Tasks collection. Tool records appear only in Task execution details.
- Needs you provides confirmation, approval and question forms. Failed or uncertain work exposes the existing explicit recovery commands. Reconnecting never automatically resubmits a command.

There is no End Goal action in the Web UI. Goal creation and lifecycle configuration remain outside this interface. Simple conversation and task delegation are decided by the core agent, not by the browser.

## Navigation and sources

The official Sidebar provides assistant, spaces, Tasks, Following, Needs you and Sources navigation. Search also exposes system records for processing inspection and recovery. Tasks and Signals have separate status filters; Sources excludes system collections. Deleted records are omitted.

Selections use the `context` and `view` URL parameters and support reload/back/forward. Missing and restricted records stay explicit. Sources show public summaries, readable messages and email bodies; unfamiliar records and public state can be expanded as structured data. The UI does not import integration, Actor or persistence implementations.

InputGroup, Field, Item, ToggleGroup, Sheet, Tabs, Alert, Empty and Skeleton provide shared controls and feedback. Mobile navigation and Activity use accessible modal sheets. Dates use the browser timezone, and reduced-motion preferences are respected.

## Application API and organization

The root Effect Atom registry shares `ApplicationClient` through AtomRpc. `/api/rpc` supplies typed queries, mutations and `SubscribeInvalidations`. Committed query keys refresh mounted queries; reconnect also refreshes data. This is live data invalidation, not token, thinking or tool-call streaming. The ordinary app does not query runtime inspection.

- `src/api`: public RPC client, invalidation subscription, shared state and timeline pagination.
- `src/assistant`: composer and searchable collections.
- `src/goals`: natural conversation, Activity and turn recovery.
- `src/tasks`: typed execution details and recovery controls.
- `src/contexts`: display projections, navigation, source records and processing recovery.
- `src/approvals`: human decision and input forms.
- `src/components`: shared presentation and official shadcn primitives.

## Font

`public/fonts/geist-variable.ttf` is Geist from [Google Fonts](https://github.com/google/fonts/tree/main/ofl/geist), distributed under the accompanying SIL Open Font License. The app serves it locally without a third-party font request.

## Validation

`pnpm test:web` builds the workspace and runs Playwright against isolated fixture transports and a real local HTTP/RPC server with fake domain services. It never invokes real models or integrations. Install Chromium with `pnpm --dir apps/web exec playwright install chromium`, or use installed Chrome with `PLAYWRIGHT_CHANNEL=chrome pnpm test:web`.

Tests cover natural messages, consecutive inputs, draft/reference editing, history pagination, authoritative Task membership, typed execution details, domain filters, approvals, retained recovery identities, invalidation reconnect, public projection errors and desktop/mobile navigation. `test/fixtures.js` contains test-only data shaped like current public projections.
