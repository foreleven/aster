import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import type { WorkspaceEnvironment } from "@aster/core/agent/session";

const workspaceSegments = (id: string): readonly string[] =>
  id.split("/").filter((segment) => segment.length > 0 && segment !== "." && segment !== "..");

/** Creates the local durable workspace environment. Isolation is a later pi-durable extension. */
export const localWorkspaceEnvironment =
  (root: string): WorkspaceEnvironment =>
  async (target) => {
    const workspace = join(root, ...workspaceSegments(target.cwd ?? "/owners"));
    await mkdir(workspace, { recursive: true });
    return new NodeExecutionEnv({ cwd: workspace });
  };
