import { Config, ConfigProvider, Schema } from "effect";

const Secret = Schema.RedactedFromValue(Schema.String.check(Schema.isPattern(/\S/)));

/** Only credential fields opt in to reference expansion; prompts remain literal. */
export const secretConfig = (value: string, provider: ConfigProvider.ConfigProvider) => {
  const reference = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/.exec(value.trim());
  return reference
    ? Config.schema(Secret, ["secrets", reference[1]!]).parse(provider)
    : Config.schema(Secret, "value").parse(
        ConfigProvider.fromUnknown({ value }, { preserveEmptyStrings: true }),
      );
};
