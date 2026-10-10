import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigProvider, Effect, Layer, Logger, Redacted } from "effect";
import { choice, SystemOneClient } from "@aster/core";
import { LocalConfig, makeSystemOneClient, SystemOneClientLive } from "../src/index.js";
import { resolveSystemOneUrl } from "../src/system-one/url.js";

const config = { model: "test", apiKey: "test-bearer-key" };
const request = { state: "Evidence", questions: { intent: choice("Choose", { yes: "Yes" }) } };
const response = () =>
  Response.json({
    answers: { intent: { type: "choice", choice: "yes" } },
    usage: { input_tokens: 0, output_tokens: 0 },
  });

test("URL references support full endpoints, multiple/repeated substitutions and literal referenced values", async () => {
  const provider = ConfigProvider.fromUnknown({
    secrets: {
      ENDPOINT: "https://example.test/v1/systemone?ak=full",
      HOST: "example.test",
      AK: "a&b=1 +/#${LITERAL}",
    },
  });
  const complete = await Effect.runPromise(resolveSystemOneUrl("${ENDPOINT}", provider));
  assert.equal(Redacted.value(complete), "https://example.test/v1/systemone?ak=full");
  const resolved = await Effect.runPromise(
    resolveSystemOneUrl("https://${HOST}/v1/systemone?ak=${AK}&copy=${AK}&mode=fast", provider),
  );
  const url = new URL(Redacted.value(resolved));
  assert.equal(url.hostname, "example.test");
  assert.equal(url.searchParams.get("ak"), "a&b=1 +/#${LITERAL}");
  assert.equal(url.searchParams.get("copy"), url.searchParams.get("ak"));
  assert.equal(url.searchParams.get("mode"), "fast");
  assert.equal(String(resolved), "<redacted>");
});

for (const suffix of ["", "/", "/v1", "/v1/systemone", "/v1/systemone/"]) {
  test(`query authentication reaches the final SDK endpoint without altering its path: ${suffix || "root"}`, async () => {
    let calls = 0;
    const url = `https://example.test/proxy${suffix}?ak=a%26b%3D1&region=cn&region=us`;
    const client = makeSystemOneClient({ ...config, url }, async (input, init) => {
      calls++;
      const target = new URL(String(input));
      assert.equal(target.pathname, "/proxy/v1/systemone");
      assert.equal(target.searchParams.get("ak"), "a&b=1");
      assert.deepEqual(target.searchParams.getAll("region"), ["cn", "us"]);
      assert.equal(new Headers(init?.headers).get("Authorization"), "Bearer test-bearer-key");
      assert.equal(init?.method, "POST");
      return response();
    });
    assert.equal((await Effect.runPromise(client.systemOne(request))).answers.intent.choice, "yes");
    assert.equal(calls, 1);
  });
}

test("System One Layer expands captured environment/.env URL references with process precedence", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "aster-systemone-url-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configPath = join(dir, "app.yaml"),
    envPath = join(dir, ".env");
  await writeFile(
    configPath,
    "config:\n  system-one:\n    url: https://example.test/v1/systemone?ak=${AK}\n    model: test\n    apiKey: literal-bearer\n",
  );
  await writeFile(envPath, "AK=dotenv-token\n");
  const before = { ...process.env };
  for (const environment of [{}, { AK: "captured-process-token" }]) {
    const source = LocalConfig.layer({ configPath, envPath, projectRoot: dir, environment });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const provider = yield* ConfigProvider.ConfigProvider;
          const url = yield* resolveSystemOneUrl(
            "https://example.test/v1/systemone?ak=${AK}",
            provider,
          );
          assert.equal(
            new URL(Redacted.value(url)).searchParams.get("ak"),
            environment.AK ?? "dotenv-token",
          );
          const client = yield* SystemOneClient.pipe(Effect.provide(SystemOneClientLive.layer));
          assert.equal(client.configured, true);
        }).pipe(Effect.provide(source)),
      ),
    );
  }
  assert.ok(
    Object.keys(process.env).length === Object.keys(before).length &&
      Object.entries(before).every(([name, value]) => process.env[name] === value),
    "Configuration acquisition must not mutate the process environment",
  );
});

for (const secrets of [{}, { AK: "" }, { AK: " " }]) {
  test(`missing/blank URL reference fails acquisition before any request: ${JSON.stringify(secrets)}`, async () => {
    const sources = ConfigProvider.layer(
      ConfigProvider.fromUnknown(
        {
          config: {
            "system-one": { ...config, url: "https://example.test/v1/systemone?ak=${AK}" },
          },
          secrets,
        },
        { preserveEmptyStrings: true },
      ),
    );
    const result = await Effect.runPromise(
      Effect.scoped(
        SystemOneClient.pipe(
          Effect.provide(SystemOneClientLive.layer.pipe(Layer.provide(sources))),
          Effect.result,
        ),
      ),
    );
    assert.equal(result._tag, "Failure");
  });
}

test("query/bearer credentials are redacted from SDK retry logs and failure diagnostics", async () => {
  const token = "PRIVATE_QUERY_TOKEN&extra=1";
  const logs: string[] = [];
  const logger = Logger.make<unknown, void>((entry) => {
    logs.push(JSON.stringify(entry.message));
  });
  let calls = 0;
  const client = makeSystemOneClient(
    { ...config, url: `https://example.test/v1/systemone?ak=${encodeURIComponent(token)}` },
    async (input) => {
      calls++;
      return Response.json(
        { error: { message: `Failure at ${input}; token=${token}; bearer=${config.apiKey}` } },
        { status: calls === 1 ? 503 : 400, headers: { "retry-after-ms": "0" } },
      );
    },
  );
  const result = await Effect.runPromise(
    client.systemOne(request).pipe(Effect.result, Effect.provide(Logger.layer([logger]))),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag !== "Failure") return;
  assert.match(result.failure.message, /400/);
  assert.ok(Redacted.isRedacted(result.failure.cause));
  const output = logs.join("\n") + JSON.stringify(result.failure);
  assert.ok(!output.includes(token));
  assert.ok(!output.includes(encodeURIComponent(token)));
  assert.ok(!output.includes(config.apiKey));
  assert.match(output, /redacted/);
  assert.equal(calls, 2);
});

test("embedded URL credentials and fragments remain invalid", () => {
  for (const url of [
    "https://user:pass@example.test/v1/systemone",
    "https://example.test/v1/systemone?ak=value#fragment",
  ])
    assert.throws(
      () => makeSystemOneClient({ ...config, url }),
      /without embedded credentials or fragment/,
    );
});

for (const query of ["", "?ak=query-token"]) {
  test(`omitting apiKey sends no Authorization header and preserves URL authentication: ${query || "anonymous"}`, async () => {
    const client = makeSystemOneClient(
      { model: "test", url: `https://example.test/v1/systemone${query}` },
      async (input, init) => {
        assert.equal(new Headers(init?.headers).has("Authorization"), false);
        assert.equal(new Headers(init?.headers).get("Content-Type"), "application/json");
        assert.equal(new URL(String(input)).search, query);
        assert.equal(JSON.parse(String(init?.body)).model, "test");
        return response();
      },
    );
    assert.equal((await Effect.runPromise(client.systemOne(request))).answers.intent.choice, "yes");
  });
}

test("System One Layer accepts a URL credential without an apiKey setting", async () => {
  const sources = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      config: {
        "system-one": { model: "test", url: "https://example.test/v1/systemone?ak=${QUERY_KEY}" },
      },
      secrets: { QUERY_KEY: "query-token" },
    }),
  );
  const client = await Effect.runPromise(
    Effect.scoped(
      SystemOneClient.pipe(Effect.provide(SystemOneClientLive.layer.pipe(Layer.provide(sources)))),
    ),
  );
  assert.equal(client.configured, true);
});

test("an explicitly configured apiKey reference remains required even when URL authentication exists", async () => {
  const sources = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      config: {
        "system-one": {
          model: "test",
          url: "https://example.test/v1/systemone?ak=query-token",
          apiKey: "${MISSING_BEARER_KEY}",
        },
      },
    }),
  );
  const result = await Effect.runPromise(
    Effect.scoped(
      SystemOneClient.pipe(
        Effect.provide(SystemOneClientLive.layer.pipe(Layer.provide(sources))),
        Effect.result,
      ),
    ),
  );
  assert.equal(result._tag, "Failure");
});
