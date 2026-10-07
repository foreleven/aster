import { Effect } from "effect";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { makeCodexAgent, agentEnvironment } from "../src/index.js";

const isInterrupted = (error: unknown) =>
  error instanceof Error && /interrupt/i.test(error.message);

test("Codex native session/run, approval response, result and recovery use app-server", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-codex-test-"));
  const originalKey = process.env.TEST_INTEGRATION_TOKEN;
  const executable = join(dir, "fake-codex");
  await writeFile(
    executable,
    `#!/usr/bin/env node
if (process.env.TEST_INTEGRATION_TOKEN) throw new Error("Project credential leaked")
const fs = require("node:fs"), readline = require("node:readline")
const file = require('node:path').join(__dirname,'state.json')
let state = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file)) : { status: 'inProgress', turn: 'run-1' }
const send = value => process.stdout.write(JSON.stringify(value)+'\\n')
readline.createInterface({input:process.stdin}).on('line', line => {
 const m=JSON.parse(line), reply=result=>send({id:m.id,result})
 if(m.id===900 && !m.method) { if(m.result.decision!=='accept') throw new Error('Wrong approval'); state.status='completed'; fs.writeFileSync(file,JSON.stringify(state)); return }
 switch(m.method) {
 case 'initialize': return reply({})
 case 'thread/start': if(m.params.ephemeral!==false) throw new Error('Ephemeral task'); return reply({thread:{id:'session-1'}})
 case 'turn/start':
   if(!m.params.input[0].text.includes('Prepared') && !m.params.input[0].text.includes('Continue')) throw new Error('Missing Task');
   state.status='inProgress'; fs.writeFileSync(file,JSON.stringify(state));
   reply({turn:{id:state.turn}});
   return send({id:900,method:'item/commandExecution/requestApproval',params:{threadId:'session-1',turnId:state.turn,command:'test'}})
 case 'thread/read': return reply({thread:{id:'session-1',turns:[{id:state.turn,status:state.status,items:[{type:'agentMessage',text:'Final result'}]}]}})
 case 'thread/resume': return reply({thread:{id:'session-1'}})
 }
})
`,
    { mode: 0o700 },
  );
  process.env.TEST_INTEGRATION_TOKEN = "test-only";
  const agent = makeCodexAgent(
    agentEnvironment({ values: process.env, privateKeys: ["TEST_INTEGRATION_TOKEN"] }),
    executable,
    join(dir, "tasks"),
  );
  let restored: ReturnType<typeof makeCodexAgent> | undefined;
  try {
    const session = await Effect.runPromise(agent.submit({ instructions: "Prepared", input: [] }));
    assert.equal(session.sessionId, "session-1");
    assert.equal(session.runId, "run-1");
    let status = await Effect.runPromise(agent.status(session));
    for (let i = 0; i < 30 && status.state !== "waiting_input"; i++) {
      await new Promise((r) => setTimeout(r, 5));
      status = await Effect.runPromise(agent.status(session));
    }
    assert.equal(status.state, "waiting_input");
    await Effect.runPromise(agent.respond(session, status.requests![0]!, { decision: "approve" }));
    assert.deepEqual((await Effect.runPromise(agent.wait(session))).result, {
      text: "Final result",
    });
    await Effect.runPromise(agent.close());
    restored = makeCodexAgent(
      agentEnvironment({ values: process.env, privateKeys: ["TEST_INTEGRATION_TOKEN"] }),
      executable,
      join(dir, "tasks"),
    );
    assert.equal((await Effect.runPromise(restored.status(session))).state, "completed");
    assert.deepEqual(await Effect.runPromise(restored.resume(session)), session);
    const followUp = await Effect.runPromise(
      restored.followUp(session, { text: "Prepared follow-up", requestId: "follow-up" }),
    );
    assert.equal(followUp.sessionId, session.sessionId);
    assert.equal(typeof followUp.metadata?.processId, "number");
    assert.notEqual(followUp.metadata?.processId, session.metadata?.processId);
    process.kill(Number(followUp.metadata?.processId), 0);
  } finally {
    await Effect.runPromise(agent.close());
    if (restored) await Effect.runPromise(restored.close());
    if (originalKey === undefined) delete process.env.TEST_INTEGRATION_TOKEN;
    else process.env.TEST_INTEGRATION_TOKEN = originalKey;
    await rm(dir, { recursive: true, force: true });
  }
});

test("Codex resumes interrupted shutdown work but preserves user cancellation", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-codex-recovery-"));
  const executable = join(dir, "fake-codex");
  await writeFile(
    executable,
    `#!/usr/bin/env node
const fs = require('node:fs'), readline = require('node:readline')
const file = require('node:path').join(__dirname, 'state.json')
let state = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file)) : { status: 'inProgress', turn: 'run-0' }
const save = () => fs.writeFileSync(file, JSON.stringify(state))
const send = value => process.stdout.write(JSON.stringify(value)+'\\n')
process.on('SIGTERM', () => { if(state.status === 'inProgress') { state.status='interrupted'; save() } process.exit(0) })
readline.createInterface({input:process.stdin}).on('line', line => {
 const m=JSON.parse(line), reply=result=>send({id:m.id,result})
 switch(m.method) {
 case 'initialize': return reply({})
 case 'thread/start': return reply({thread:{id:'session-1'}})
 case 'turn/start': state={status:'inProgress',turn:state.turn==='run-0'?'run-1':'run-2'}; save(); return reply({turn:{id:state.turn}})
 case 'thread/read': state=JSON.parse(fs.readFileSync(file)); return reply({thread:{turns:[{id:state.turn,status:state.status,items:[]}]}})
 case 'thread/resume': return reply({thread:{id:'session-1'}})
 }
})
`,
    { mode: 0o700 },
  );
  const agent = makeCodexAgent({ ...process.env }, executable, join(dir, "tasks"));
  const restored = makeCodexAgent({ ...process.env }, executable, join(dir, "tasks"));
  try {
    const session = await Effect.runPromise(agent.submit({ instructions: "Prepared", input: [] }));
    assert.equal((await Effect.runPromise(agent.status(session))).state, "running");
    await Effect.runPromise(agent.close());
    assert.equal((await Effect.runPromise(restored.status(session))).resumable, true);
    const resumed = await Effect.runPromise(restored.resume(session));
    assert.equal(resumed.sessionId, session.sessionId);
    assert.equal(resumed.runId, "run-2");
    await writeFile(
      join(dir, "state.json"),
      JSON.stringify({ status: "interrupted", turn: "run-2" }),
    );
    assert.equal((await Effect.runPromise(restored.status(resumed))).state, "cancelled");
    await assert.rejects(Effect.runPromise(restored.resume(resumed)), /interrupted/);
  } finally {
    await Effect.runPromise(agent.close());
    await Effect.runPromise(restored.close());
    await rm(dir, { recursive: true, force: true });
  }
});

test("Codex cancellation stops RPC chains before the next side effect and does not poison the connection", async () => {
  const { readFile, access } = await import("node:fs/promises");
  const { setTimeout: delay } = await import("node:timers/promises");
  const dir = await mkdtemp(join(tmpdir(), "aster-codex-cancel-"));
  const executable = join(dir, "fake-codex");
  const log = join(dir, "rpc.jsonl");
  await writeFile(
    executable,
    `#!/usr/bin/env node
const fs = require('node:fs'), readline = require('node:readline'), path = require('node:path');
const log = path.join(__dirname, 'rpc.jsonl');
const send = value => process.stdout.write(JSON.stringify(value)+'\\n');
readline.createInterface({input:process.stdin}).on('line', line => {
 const m=JSON.parse(line); fs.appendFileSync(log, JSON.stringify(m)+'\\n');
 const reply=result=>send({id:m.id,result});
 if(m.method==='initialize') reply({});
 if(m.method==='thread/start') setTimeout(()=>reply({thread:{id:'session-'+m.id}}),100);
 if(m.method==='turn/start') setTimeout(()=>reply({turn:{id:'run-'+m.id}}),100);
 if(m.method==='thread/read') setTimeout(()=>reply({thread:{turns:[]}}),100);
});
`,
    { mode: 0o700 },
  );
  const agent = makeCodexAgent({ ...process.env }, executable, join(dir, "tasks"));
  const methods = async (): Promise<string[]> =>
    (await readFile(log, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line).method);
  const until = async (predicate: () => Promise<boolean>) => {
    for (let i = 0; i < 200; i++) {
      if (await predicate().catch(() => false)) return;
      await delay(5);
    }
    throw new Error("mock RPC did not arrive");
  };
  try {
    await assert.rejects(
      Effect.runPromise(agent.submit({ instructions: "cancelled", input: [] }), {
        signal: AbortSignal.abort(new Error("already cancelled")),
      }),
      isInterrupted,
    );
    await assert.rejects(access(log));
    const controller = new AbortController();
    const submission = Effect.runPromise(
      agent.submit({ instructions: "cancel during thread", input: [] }),
      { signal: controller.signal },
    );
    const rejected = assert.rejects(submission, isInterrupted);
    await until(async () => (await methods()).includes("thread/start"));
    controller.abort();
    await rejected;
    await delay(150);
    assert.equal((await methods()).includes("turn/start"), false);
    const session = await Effect.runPromise(agent.submit({ instructions: "live", input: [] }));
    assert.ok(session.sessionId);
    assert.equal((await methods()).filter((m) => m === "initialize").length, 1);
    const reading = new AbortController();
    const status = assert.rejects(
      Effect.runPromise(agent.status(session), { signal: reading.signal }),
      isInterrupted,
    );
    await until(async () => (await methods()).includes("thread/read"));
    reading.abort();
    await status;
    const before = (await methods()).length;
    await assert.rejects(
      Effect.runPromise(agent.resume(session), {
        signal: AbortSignal.abort(new Error("stop resume")),
      }),
      isInterrupted,
    );
    assert.equal((await methods()).length, before);
    const executing = new AbortController();
    const turn = assert.rejects(
      Effect.runPromise(agent.submit({ instructions: "cancel during turn", input: [] }), {
        signal: executing.signal,
      }),
      isInterrupted,
    );
    await until(async () => (await methods()).filter((m) => m === "turn/start").length === 2);
    executing.abort();
    await turn;
    await delay(150);
    assert.equal((await methods()).filter((m) => m === "turn/start").length, 2);
  } finally {
    await Effect.runPromise(agent.close());
    await rm(dir, { recursive: true, force: true });
  }
});

test("Codex steers active work and starts a new turn in the same thread after completion", async () => {
  const { readFile } = await import("node:fs/promises");
  const dir = await mkdtemp(join(tmpdir(), "aster-codex-followup-"));
  const executable = join(dir, "fake-codex");
  await writeFile(
    executable,
    `#!/usr/bin/env node
const fs = require('node:fs'), readline = require('node:readline'), path = require('node:path');
const log = path.join(__dirname, 'rpc.jsonl');
let turn = 0;
const send = value => process.stdout.write(JSON.stringify(value)+'\\n');
readline.createInterface({input:process.stdin}).on('line', line => {
 const m=JSON.parse(line); fs.appendFileSync(log, JSON.stringify(m)+'\\n');
 const reply=result=>send({id:m.id,result});
 if(m.method==='initialize') reply({});
 if(m.method==='thread/start' || m.method==='thread/resume') reply({thread:{id:'thread-1'}});
 if(m.method==='turn/start') reply({turn:{id:'turn-'+(++turn)}});
 if(m.method==='turn/steer') reply({turnId:m.params.expectedTurnId});
 if(m.method==='thread/read') reply({thread:{turns:[{id:'turn-'+turn,status:fs.existsSync(path.join(__dirname,'completed'))?'completed':'inProgress',items:[]}]}});
});
`,
    { mode: 0o700 },
  );
  const agent = makeCodexAgent({ ...process.env }, executable, join(dir, "tasks"));
  try {
    const first = await Effect.runPromise(
      agent.submit({ instructions: "Initial work", input: [] }),
    );
    assert.deepEqual(
      await Effect.runPromise(agent.followUp(first, { requestId: "busy", text: "Add regions" })),
      first,
    );
    await writeFile(join(dir, "completed"), "");
    const next = await Effect.runPromise(
      agent.followUp(first, { requestId: "later", text: "Extend report" }),
    );
    assert.equal(next.sessionId, first.sessionId);
    assert.notEqual(next.runId, first.runId);
    const calls = (await readFile(join(dir, "rpc.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.equal(calls.filter((call) => call.method === "thread/start").length, 1);
    assert.equal(calls.filter((call) => call.method === "turn/start").length, 2);
    const steer = calls.find((call) => call.method === "turn/steer");
    assert.equal(steer.params.expectedTurnId, first.runId);
    assert.equal(steer.params.input[0].text, "Add regions");
  } finally {
    await Effect.runPromise(agent.close());
    await rm(dir, { recursive: true, force: true });
  }
});
