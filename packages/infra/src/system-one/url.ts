import { secretConfig } from "@aster/core";
import { ConfigProvider, Effect, Redacted } from "effect";

/** Endpoint references opt in explicitly; prompt/configuration strings remain literal. */
export const resolveSystemOneUrl = Effect.fn("SystemOne.resolveUrl")(function* (
  template: string,
  provider: ConfigProvider.ConfigProvider,
) {
  if (/^\$\{[A-Za-z_][A-Za-z0-9_]*\}$/.test(template.trim()))
    return yield* secretConfig(template, provider);
  const pattern = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;
  const references = [...new Set([...template.matchAll(pattern)].map((match) => match[1]!))];
  const resolved = new Map(
    yield* Effect.forEach(references, (name) =>
      secretConfig(`\${${name}}`, provider).pipe(Effect.map((value) => [name, value] as const)),
    ),
  );
  const queryStart = template.indexOf("?");
  // Replace the template once: values containing another ${...} are literal credentials.
  const url = template.replace(pattern, (_reference, name: string, offset: number) => {
    const value = Redacted.value(resolved.get(name)!);
    return queryStart >= 0 && offset > queryStart ? encodeURIComponent(value) : value;
  });
  return Redacted.make(url);
});
