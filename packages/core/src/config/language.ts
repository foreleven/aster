import { Config, Schema } from "effect";

export const DEFAULT_OUTPUT_LANGUAGE = "zh-CN";

/** Shared default for model-authored prose; independent of prompt/source language. */
export const outputLanguage = Config.schema(
  Schema.String.check(
    Schema.makeFilter((value) => /^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$/.test(value.trim())),
  ),
  ["config", "language"],
).pipe(
  Config.withDefault(DEFAULT_OUTPUT_LANGUAGE),
  Config.map((value) => value.trim()),
);

export const outputLanguageInstruction = (language: string) =>
  `Write natural-language output in ${language}, including replies, summaries, explanations and human-readable tool fields. The language of prompts, configuration descriptions or source evidence does not change this output language. Follow an explicit user request for a different language for a specific deliverable. Preserve code, verbatim quotations, tool names, JSON field names, identifiers and enum values; do not translate machine-readable values.`;
