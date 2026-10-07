export const at = "2026-09-29T12:30:00Z";
export const taskPath = `/tasks/${"a".repeat(64)}`;
const context = (path, description, state, messages = []) => ({
  path,
  description,
  state,
  messages,
  revision: 1,
  projection: { visibility: "public" },
});
export function fixture() {
  return {
    contexts: [
      context("/goals/personal", "Personal assistant", {
        title: "Personal assistant",
        status: "active",
        summary: "",
        tasks: [],
      }),
      context("/goals/engine", "Keep up with the Knowledge Engine project", {
        title: "Knowledge Engine",
        status: "active",
        summary: "Validation starts this week.",
        tasks: [taskPath],
      }),
      context(taskPath, "Review the release", {
        status: "waiting_input",
        sourcePath: "/signals/release",
        replyTo: "/goals/engine",
        agent: "internal",
        inputs: 1,
      }),
      context("/signals/release", "Daily release check", {
        owner: "/goals/engine",
        status: "active",
        version: 1,
        trigger: {
          _tag: "Schedule",
          schedule: { type: "cron", expression: "0 9 * * *", timeZone: "Asia/Shanghai" },
        },
        nextDue: "2026-10-08T09:00:00+08:00",
        task: {
          _tag: "Agent",
          replyTo: "/goals/engine",
          task: {
            instructions: "Review the latest release evidence",
            input: [{ content: "Release notes", sources: ["/sources/release"] }],
          },
        },
      }),
      context("/signals/watch", "Watch release changes", {
        owner: "/goals/engine",
        status: "paused",
        version: 1,
        trigger: { _tag: "Context", when: "Release readiness changes" },
        task: { _tag: "Goal", target: "/goals/engine", text: "Review the change" },
      }),
      context("/signals/removed", "Removed reminder", {
        status: "deleted",
        version: 1,
        trigger: { _tag: "Context", when: "Old condition" },
        task: { _tag: "Goal", target: "/goals/engine", text: "Old task" },
      }),
      context(
        "/sources/release",
        "Release notes",
        { summary: "Integration is complete. Validation starts this week." },
        [{ text: "Frontend checks are ready.", at }],
      ),
      context("/approvals", "Approvals", {}),
      context("/system-one", "Context matching", { work: [] }),
    ],
    approvals: [
      {
        id: "approval-1",
        contextPath: taskPath,
        kind: "approval",
        status: "pending",
        request: { id: "approval-1", kind: "approval", prompt: "May I run release verification?" },
      },
      {
        id: "question-1",
        contextPath: taskPath,
        kind: "input",
        status: "pending",
        request: {
          id: "question-1",
          kind: "input",
          prompt: "Choose the environment",
          questions: [
            { id: "environment", prompt: "Which environment?", options: ["Test", "Production"] },
          ],
        },
      },
    ],
    timelines: {
      personal: [],
      engine: [
        { id: 1, role: "user", text: "What changed this week?", at },
        {
          id: 2,
          role: "assistant",
          text: "**Integration is complete.** Validation starts this week.",
          at,
        },
      ],
    },
    tasks: {
      [taskPath]: {
        path: taskPath,
        revision: 1,
        agent: "internal",
        status: "waiting_input",
        instructions: "Review the latest release evidence",
        sources: ["/sources/release"],
        hasExecution: true,
        messages: [
          { id: 1, kind: "instruction", text: "Review release evidence", at },
          { id: 2, kind: "tool-call", text: "Read public release context", at },
        ],
        requests: [],
      },
    },
    processing: {
      owner: "system-one",
      revision: 1,
      entries: [
        {
          id: "evidence",
          kind: "screening",
          source: "/sources/release",
          target: "/system-one",
          status: "completed",
          matches: [{ _tag: "NotMatched", target: "/goals/engine", reason: "Already handled" }],
        },
      ],
    },
  };
}
