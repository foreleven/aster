# Goals UI design QA

Date: 2026-10-01.

## Comparison target and evidence

- Source visual: `/Users/feng/.codex/attachments/17b0a3d1-0b28-47db-a3a1-7551c244df72/image-1.png`.
- Implementation: isolated production-build preview at `http://127.0.0.1:4330/`, using `apps/web/test/fixtures.js` records. Production never imports these fixtures.
- Desktop capture: `/Users/feng/.codex/visualizations/2026/09/30/01a0f304-fe20-7900-884a-310012001654/goals-ui/goals-desktop.png`.
- Full-view comparison: the same directory's `comparison.png`, source on the left and implementation on the right.
- Focused comparisons: `comparison-header.png`, `comparison-work-panel.png`, and `comparison-timeline.png` in that directory.
- Both source and desktop screenshot are 1487 × 1058 pixels; browser viewport is 1487 × 1058 CSS pixels at device scale factor 1. No density normalization was required. Side-by-side comparisons preserve each image's original dimensions.
- State: light theme, Hokkaido Goal selected, Timeline / All events, three tasks and two Signals. Source dates are represented by static fixtures; the browser displays timestamps in its local timezone. “Today” is intentionally not claimed for historical dates.
- Responsive evidence: `/tmp/aster-goals-responsive-1280.png`, `/tmp/aster-goals-responsive-1024.png`, and `/tmp/aster-goals-responsive-390.png`; these use an intentionally long Goal title. A copy of the phone capture is `goals-mobile.png` beside the desktop capture.

The Codex in-app browser was used for desktop and 390 × 844 mobile visual inspection, timeline filtering, Notes navigation, and execution inspection. The final production-build preview had no console errors or warnings. Playwright captures corroborate the same rendered layout and provide reproducible saved evidence.

## Findings and comparison history

1. Initial implementation: **P1**, title/action overlap and duplicated conversation rendering. Fixed with a wrapping title/action layout and a single native-history timeline.
2. Intermediate comparison: **P2**, task cards were too tall, pushing monitoring context below the fold; timeline paragraph spacing inherited an older rule. Removed redundant execution footer links in favor of clickable card titles, tightened card spacing, and scoped timeline typography. The final desktop and focused captures show the corrected density.
3. Mobile comparison: **P2**, the long composer placeholder clipped onto a second line. Increased mobile textarea height; the in-app phone capture shows the complete placeholder. Tasks and Signals remain reachable below the conversation, with a direct anchor from the header. The desktop composer stays visible while history scrolls.
4. Final comparison: no remaining actionable P0/P1/P2 visual findings within the implemented Goals scope.

## Required fidelity surfaces

- **Fonts and typography:** Arial/system sans-serif reproduces the reference's plain UI typography. The 30px Goal title, 20px section headings, 13–16px content, and muted metadata preserve hierarchy. Long titles and messages wrap without horizontal overflow.
- **Spacing and layout:** the desktop columns are 279px / flexible / 332px, matching the reference's major divisions. Tabs, breadcrumb bar, timeline rail, card borders, and bottom composer align with the source composition. Removing the four excluded navigation items intentionally moves the Goal list upward.
- **Colors and tokens:** white content, pale gray navigation, blue selection/action accents, and green/amber/blue status badges match the reference. Task icons use green, waiting icons amber, and uncertain execution icons purple.
- **Assets:** the interface uses the existing Lucide library for vector UI icons, including its Asterisk brand mark. No handwritten image approximations or generated travel thumbnails are shipped. The real API has no Goal icon, thumbnail, or attachment-preview contract; evidence remains an expandable, working Context reference.
- **Copy and content:** production shows actual public Contexts and Goal history. The Hokkaido material is test-only. Status, progress, evidence, and execution outcomes are not fabricated.

## Intentional product constraints

- Home, Search, Library, and Settings are omitted as requested.
- Direct editing, pausing, and separate archiving are visibly disabled and explained in Details. Existing End Goal is available through a confirmation dialog; completed Goals are grouped under Archived.
- Missing creation dates, account identity, category metadata, “last checked” timestamps, and historical event-status snapshots are not invented. Source media thumbnails and per-event action menus have no current API counterpart.
- The source's exact custom logo, goal-specific icons, and raster previews remain possible future design work when corresponding product assets and data contracts exist. Current library icons and text references are deliberate adaptations.

## Verification and checklist

- [x] Workspace build and `pnpm test:web`: 15 browser tests passed, including a real isolated HTTP/SSE server.
- [x] Final frontend build and targeted reference/responsive tests after visual adjustments.
- [x] `pnpm check`: lint, formatting, and workspace typechecks passed.
- [x] Effect language-service diagnostics for `apps/web/tsconfig.json`: zero errors and warnings.
- [x] Verified sending, failed-send draft retention, no automatic mutation retries, completion confirmation, history paging, filters, context inspection, scoped approvals, SSE recovery, and narrow layouts.
- [x] Preview runs on a dedicated loopback port; temporary preview servers on Aster's default port were stopped. No real integrations, paid models, runtime data, or credentials were used.

The build reports Vite's non-blocking JavaScript chunk-size advisory (main chunk approximately 555 kB before gzip). This is a performance follow-up, not a visual defect.

**Follow-up polish:** exact supplied brand assets and font metrics could improve pixel-level fidelity further. Live integration behavior was intentionally not exercised.

final result: passed
