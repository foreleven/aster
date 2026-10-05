import type { DescriptionPolicy } from "../reasoning/context-description.js";
import { runView } from "../tasks/view.js";

export const coreDescriptions: readonly DescriptionPolicy[] = [
  { matches: (path) => runView.matches!(path), identity: "An occurrence of a Signal" },
  { matches: (path) => /^\/goals\/[^/]+$/.test(path), identity: "Ongoing work goal" },
  {
    matches: (path) => /^\/signals\/[^/]+$/.test(path),
    identity: "Condition and schedule monitoring",
  },
  {
    matches: (path) => /^\/delegations\/[^/]+$/.test(path),
    identity: "Task delegated to an external agent",
  },
];
