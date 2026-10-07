import { goalView, goalsRootView } from "../goals/view.js";
import { signalView, signalsRootView } from "../signals/state/snapshot.js";
import { taskView, tasksRootView } from "../tasks/view.js";
import { approvalView } from "../approvals/view.js";

/** Installed before owners start, including views of dormant persisted owners. */
export const coreContextViews = [
  goalView,
  taskView,
  signalView,
  approvalView,
  goalsRootView,
  signalsRootView,
  tasksRootView,
];
