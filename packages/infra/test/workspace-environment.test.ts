import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { localWorkspaceEnvironment } from "../src/workspaces/environment.js";

test("local workspace environments resolve an opaque workspace identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "aster-workspaces-"));
  try {
    const environment = await localWorkspaceEnvironment(root)(
      { cwd: "/goals/project" },
      BACKGROUND_CONTEXT,
    );
    assert.ok(environment);
    assert.equal(environment.cwd, join(root, "goals", "project"));
    await environment.cleanup(BACKGROUND_CONTEXT);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
