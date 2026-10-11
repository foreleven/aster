import { Match, Predicate, SchemaIssue, type Schema } from "effect";

const expectedType = (issue: SchemaIssue.InvalidType) =>
  Match.value(issue.ast._tag).pipe(
    Match.when("String", () => "Expected a string"),
    Match.when("Number", () => "Expected a number"),
    Match.when("Boolean", () => "Expected a boolean"),
    Match.when("Arrays", () => "Expected an array"),
    Match.when("Objects", () => "Expected an object"),
    Match.when("Literal", () => "Expected the declared literal"),
    Match.orElse(() => "Expected the declared field type"),
  );
const formatter = SchemaIssue.makeFormatterStandardSchemaV1({
  leafHook: (issue) =>
    Match.value(issue).pipe(
      Match.tag("InvalidType", expectedType),
      Match.tag("InvalidValue", () => "Expected a valid field value"),
      Match.tag("MissingKey", () => "Required field is missing"),
      Match.tag("UnexpectedKey", () => "Unexpected field"),
      Match.tag("Forbidden", () => "Field operation is forbidden"),
      Match.tag("OneOf", () => "Expected exactly one declared alternative"),
      Match.exhaustive,
    ),
  // Read only static schema-owned expectations, never callbacks or rejected input.
  checkHook: (issue) =>
    typeof issue.filter.annotations?.expected === "string"
      ? `Expected ${issue.filter.annotations.expected}`
      : "Field does not satisfy its declared constraint",
});

/** Model errors retain useful schema locations without exposing arbitrary map keys. */
export const toolValidationFeedback = (schema: unknown) => {
  const fieldNames = new Set<string>();
  const collect = (value: unknown): void => {
    if (Array.isArray(value)) {
      for (const child of value) collect(child);
      return;
    }
    if (!Predicate.isObject(value)) return;
    if (Predicate.isObject(value.properties))
      for (const name of Object.keys(value.properties)) fieldNames.add(name);
    for (const child of Object.values(value)) collect(child);
  };
  collect(schema);
  return (error: Schema.SchemaError) => {
    const issues = formatter(error.issue).issues.map((issue) => ({
      path: (issue.path ?? []).map((part) => {
        const key = typeof part === "object" ? part.key : part;
        if (typeof key === "number") return key;
        return typeof key === "string" && fieldNames.has(key) ? key : "<key>";
      }),
      message: issue.message,
    }));
    const text = issues
      .map(
        (issue) =>
          `${issue.path.length === 0 ? "arguments" : issue.path.join(".")}: ${issue.message}`,
      )
      .join("; ");
    return {
      content: [{ type: "text" as const, text: `Invalid tool arguments: ${text}` }],
      details: { code: "invalid_arguments", message: "Invalid tool arguments", issues },
      isError: true,
    };
  };
};
