export const at = "2026-09-29T12:30:00Z";
export function fixture() {
  const context = (path, description, state = {}, messages = []) => ({
    path,
    description,
    state,
    messages,
  });
  const actor = (path, contextPath) => ({
    path,
    contextPath,
    parent: path.slice(0, path.lastIndexOf("/")),
    incarnation: path,
    status: "running",
    phase: "running",
    pendingEffects: 0,
    failures: 0,
    restarts: 0,
    processing: false,
    mailboxSize: 0,
    processed: 12,
    lastActivity: at,
  });
  return {
    at,
    runtime: {
      phase: "ready",
      actors: [
        actor("/user/lark", "/lark"),
        actor("/user/lark/im", "/lark/im"),
        actor("/user/lark/im/chat-1", "/lark/im/chats/chat-1"),
        actor("/user/goals", "/goals"),
        actor("/user/goals/engine", "/goals/engine"),
        actor("/user/signals", "/signals"),
        actor("/user/signals/progress", "/signals/progress"),
        actor("/user/signals/progress/run-1", "/signals/progress/runs/run-1"),
        actor("/user/approvals", "/approvals"),
        actor("/user/memory", "/memory"),
      ],
      events: [
        {
          _tag: "CommandProcessed",
          incarnation: "run-1",
          commandTag: "TaskPrepared",
          path: "/user/signals/progress/run-1",
          timestamp: at,
          success: true,
        },
        {
          _tag: "CommandProcessed",
          incarnation: "chat-1",
          commandTag: "Summarized",
          path: "/user/lark/im/chat-1",
          timestamp: at,
          success: true,
        },
      ],
    },
    contexts: [
      context("/lark", "My work account", { status: "ready" }),
      context("/lark/im", "Work IM", { ready: true }),
      context(
        "/lark/im/chats/chat-1",
        "Knowledge Engine · Frontend discussion",
        { summary: "Core workflow integration is complete; validation starts this week." },
        [
          {
            type: "Summary",
            text: "Core workflow integration is complete; validation starts this week.",
            at,
          },
        ],
      ),
      context(
        "/goals/engine",
        "Monitor Knowledge Engine project progress",
        { slug: "engine", status: "active", progress: "Awaiting test feedback" },
        [
          {
            type: "assistant",
            text: "Monitoring project progress. Next, track integration and validation results.",
            at,
          },
        ],
      ),
      context("/signals/progress", "Significant change in project progress", {
        goal: "engine",
        active: true,
      }),
      context(
        "/signals/progress/runs/run-1",
        "Summarize Knowledge Engine project progress",
        {
          status: "awaiting-confirmation",
          sourcePath: "/lark/im/chats/chat-1",
          definition: { goal: "engine" },
        },
        [
          { type: "Triggered", at },
          {
            type: "TaskPrepared",
            task: { instructions: "Summarize this week’s project progress", input: [] },
            at,
          },
          { type: "ConfirmationRequested", at },
        ],
      ),
      context("/approvals", "Task approval queue", {
        entries: [
          {
            id: "approval-1",
            target: "/user/signals/progress/run-1",
            contextPath: "/signals/progress/runs/run-1",
            kind: "confirmation",
            status: "pending",
            request: {
              id: "approval-1",
              kind: "approval",
              prompt:
                "Summarize this week’s Knowledge Engine integration progress and remaining validation work.",
            },
          },
          {
            id: "question-1",
            target: "/user/signals/progress/run-1",
            contextPath: "/signals/progress/runs/run-1",
            kind: "input",
            status: "pending",
            request: {
              id: "question-1",
              kind: "input",
              prompt: "Specify the release scope",
              questions: [
                {
                  id: "scope",
                  prompt: "Which environment should receive the release?",
                  options: ["Test", "Production"],
                },
              ],
            },
          },
        ],
      }),
      context("/memory", "Long-term work memory", { status: "ready" }),
      context("/signals/old/runs/old", "Historical execution record", { status: "completed" }, [
        { type: "Completed", text: "Historical result", at },
      ]),
    ],
  };
}

/** Reference-shaped public records, used only by browser tests and isolated visual QA. */
export function designFixture() {
  const data = fixture();
  const now = "2025-01-14T10:24:00+09:00";
  const goal = data.contexts.find((context) => context.path === "/goals/engine");
  goal.description = "Plan our Hokkaido trip";
  goal.state.summary =
    "Planning a 7–8 day trip to Hokkaido in late Jan 2026 for two people. We’re looking for a mix of snow activities, good food, and a relaxed pace. Currently comparing flight and hotel options, checking visa requirements, and watching for price drops. No bookings yet.";
  goal.state.tasks = [
    {
      id: "flights",
      title: "Compare flights",
      instructions: "Search for best fares from Tokyo to Sapporo (Jan 20–28)",
      status: "open",
      execution: { runPath: "/signals/progress/runs/run-1", status: "running" },
      updatedAt: now,
    },
    {
      id: "visa",
      title: "Check visa rules",
      instructions: "Confirm entry requirements for our nationality and stay duration.",
      status: "open",
      execution: { runPath: "/signals/visa/runs/run-1", status: "waiting_input" },
      updatedAt: "2025-01-14T07:24:00+09:00",
    },
    {
      id: "hotels",
      title: "Search hotel prices",
      instructions: "Find and compare hotels in Sapporo, Otaru, and Niseko.",
      status: "open",
      execution: { runPath: "/signals/hotels/runs/run-1", status: "uncertain" },
      updatedAt: "2025-01-14T08:17:00+09:00",
    },
  ];
  const event = (type, text, hour, day = "14", references = []) => ({
    type,
    text,
    at: `2025-01-${day}T${hour}:00+09:00`,
    references,
  });
  goal.messages = [
    event(
      "User",
      "Priorities: snow activities, good food (ramen, seafood), at least one onsen, and a day trip to Otaru. Keep it relaxed, not too many hotel changes.",
      "14:21",
      "13",
    ),
    event(
      "Task",
      "Check visa rules\nResearched visa requirements for Japanese citizens. No visa required for short-term stay (up to 90 days).",
      "16:42",
      "13",
    ),
    event(
      "Approval",
      "Use 7–8 day range\nApproved date range Jan 20–28, 2026 for planning.",
      "20:03",
      "13",
    ),
    event(
      "Result",
      "Hotel options found\nFound 12 options in Sapporo (¥9,000–¥15,000/night). Good availability near Susukino and Odori.",
      "22:14",
      "13",
    ),
    event(
      "Signal",
      "Local holiday availability\nChecked Hokkaido public holidays. No major holidays during our dates.",
      "07:31",
      "14",
      ["/sources/calendar"],
    ),
    event(
      "Execution",
      "Search hotel prices\nStarted checking hotel prices in Sapporo, Otaru, and Niseko for Jan 21–28.",
      "08:17",
    ),
    event(
      "User",
      "Let’s check if we can shift to Jan 21–28. The ¥3,800 fare looks good. Also compare with return on Jan 28.",
      "08:50",
    ),
    event(
      "Signal",
      "Flight price drops below ¥4,000\nFound fares from HND to CTS at ¥3,800 (ANA) for Jan 21.",
      "09:12",
      "14",
      ["/sources/flights", "/sources/skyscanner", "/sources/ana"],
    ),
    event(
      "Task",
      "Compare flights\nStarted running to search for flights from Tokyo to Sapporo (CTS) on Jan 20–28.",
      "10:24",
    ),
  ];
  goal.state.historyCount = goal.messages.length;
  const signal = data.contexts.find((context) => context.path === "/signals/progress");
  signal.description = "Flight price drops below ¥4,000";
  signal.state = {
    goal: "engine",
    active: true,
    when: "Check for round-trip fares HND/CTS below ¥4,000 for our dates.",
    schedule: { type: "cron", expression: "0 */6 * * *", timeZone: "Asia/Tokyo" },
    nextDue: Date.parse("2025-01-14T16:12:00+09:00"),
    seenSources: ["hashed-source-fingerprint"],
    occurrences: ["/sources/flights", "/sources/skyscanner", "/sources/ana"].map((path) => ({
      source: { path },
    })),
  };
  const run = data.contexts.find((context) => context.path === "/signals/progress/runs/run-1");
  run.description = "Compare flights";
  run.state.status = "running";
  data.contexts.find((context) => context.path === "/approvals").state.entries = [];
  data.contexts.push(
    {
      path: "/signals/holiday",
      description: "Local holiday availability",
      state: {
        goal: "engine",
        active: true,
        when: "Monitor Japanese and Hokkaido local holidays during our travel dates.",
        schedule: { type: "cron", expression: "0 9 * * *", timeZone: "Asia/Tokyo" },
        nextDue: Date.parse("2025-01-15T09:00:00+09:00"),
        occurrences: [{ source: { path: "/sources/calendar" } }],
      },
      messages: [],
    },
    ...[
      ["pricing", "Monitor competitor pricing", "active"],
      ["wiki", "Launch personal wiki", "active"],
      ["japanese", "Learn Japanese", "paused"],
      ["workstation", "Upgrade my workstation", "watching"],
      ["kitchen", "Plan kitchen renovation", "completed"],
      ["ev", "Explore EV options", "completed"],
    ].map(([slug, description, status]) => ({
      path: `/goals/${slug}`,
      description,
      state: { status, tasks: [] },
      messages: [],
    })),
    ...[
      ["flights", "Google Flights – HND to CTS"],
      ["skyscanner", "Skyscanner – Hokkaido"],
      ["ana", "ANA official site"],
      ["calendar", "Hokkaido Prefectural Calendar"],
    ].map(([slug, description]) => ({
      path: `/sources/${slug}`,
      description,
      state: {},
      messages: [],
    })),
    ...["visa", "hotels"].map((slug) => ({
      path: `/signals/${slug}/runs/run-1`,
      description: `Trip ${slug} execution`,
      state: { status: "waiting_input", definition: { goal: "engine" } },
      messages: [],
    })),
  );
  return data;
}
