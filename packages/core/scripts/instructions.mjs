import { readFile, writeFile } from "node:fs/promises";
const entries = await Promise.all(
  ["goal", "task", "screening"].map(async (name) => [
    name,
    await readFile(new URL(`../src/agent/${name}/instructions.md`, import.meta.url), "utf8"),
  ]),
);
await writeFile(
  new URL("../src/agent/runtime/instructions.generated.ts", import.meta.url),
  `// Generated from Assembly Markdown assets by scripts/instructions.mjs.\nexport const instructions = ${JSON.stringify(Object.fromEntries(entries), null, 2)} as const;\n`,
);
