import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer, Redacted } from "effect";
import { choice, SystemOneClient } from "@aster/core";
import { resolveSystemOneUrl, resolveSystemOneSettings } from "../src/system-one/settings.js";
import { SystemOneTransport } from "../src/system-one/layer.js";
import { http, systemOne } from "./http-fixtures.js";
const request = { state: "Evidence", questions: { intent: choice("Choose", { yes: "Yes" }) } };
const success = () => Response.json({ answers: { intent: { type: "choice", choice: "yes" } } });
test("references expand once with query encoding and captured source precedence", async () => {
  const provider = ConfigProvider.fromUnknown({
    secrets: { AK: "a&b=${LITERAL}", URL: "https://example.test?ak=full" },
  });
  assert.equal(
    Redacted.value(await Effect.runPromise(resolveSystemOneUrl("${URL}", provider))),
    "https://example.test?ak=full",
  );
  const value = await Effect.runPromise(
    resolveSystemOneSettings({ url: "https://example.test/v1?ak=${AK}", model: "test" }, provider),
  );
  assert.equal(new URL(Redacted.value(value.endpoint)).searchParams.get("ak"), "a&b=${LITERAL}");
  assert.equal(new URL(Redacted.value(value.endpoint)).pathname, "/v1/systemone");
});
for (const suffix of ["", "/", "/v1", "/v1/systemone", "/v1/systemone/"]) {
  test(`endpoint normalization and optional authentication: ${suffix}`, async () => {
    const client = await Effect.runPromise(
      systemOne(
        { url: `https://example.test/proxy${suffix}?ak=a%26b&region=cn&region=us`, model: "test" },
        async (url, init) => {
          assert.equal(new URL(String(url)).pathname, "/proxy/v1/systemone");
          assert.deepEqual(new URL(String(url)).searchParams.getAll("region"), ["cn", "us"]);
          assert.equal(new Headers(init?.headers).has("Authorization"), false);
          return success();
        },
      ),
    );
    await Effect.runPromise(client.systemOne(request));
  });
}
test("the configured Layer resolves references and keeps explicit apiKey requirements", async () => {
  for (const key of [undefined, "${KEY}"]) {
    const source = ConfigProvider.layer(
      ConfigProvider.fromUnknown({
        config: {
          "system-one": { url: "https://example.test?ak=${AK}", model: "test", apiKey: key },
        },
        secrets: { AK: "query", KEY: "bearer" },
      }),
    );
    await Effect.runPromise(
      SystemOneClient.use((client) => client.systemOne(request)).pipe(
        Effect.provide(
          SystemOneTransport.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                source,
                http(async (_url, init) => {
                  assert.equal(
                    new Headers(init?.headers).get("Authorization"),
                    key ? "Bearer bearer" : null,
                  );
                  return success();
                }),
              ),
            ),
          ),
        ),
      ),
    );
  }
  for (const config of [
    { url: "https://x.test", model: "test", apiKey: "${MISSING}" },
    { url: "https://x.test#fragment", model: "test" },
    { url: "https://user:password@x.test", model: "test" },
  ])
    assert.equal(
      (
        await Effect.runPromise(
          resolveSystemOneSettings(config, ConfigProvider.fromUnknown({})).pipe(Effect.result),
        )
      )._tag,
      "Failure",
    );
});
