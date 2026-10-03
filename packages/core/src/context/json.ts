import { Predicate, Record, Schema } from "effect";

// Optional object properties mean absence on the public JSON boundary. Keep array
// positions and all other values intact so invalid data still fails validation.
const omitUndefinedProperties = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(omitUndefinedProperties);
  if (
    Predicate.isObject(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  ) {
    return Record.map(
      Record.filter(value as Record<string, unknown>, Predicate.isNotUndefined),
      omitUndefinedProperties,
    );
  }
  return value;
};

export const publicJson = (value: unknown): Schema.Json =>
  Schema.decodeUnknownSync(Schema.Json)(omitUndefinedProperties(value));

export const undefinedPaths = (value: unknown, prefix = ""): readonly string[] => {
  if (Array.isArray(value))
    return value.flatMap((item, index) => undefinedPaths(item, `${prefix}[${index}]`));
  if (
    Predicate.isObject(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  ) {
    return Object.entries(value).flatMap(([key, item]) => {
      const path = prefix ? `${prefix}.${key}` : key;
      return item === undefined ? [path] : undefinedPaths(item, path);
    });
  }
  return [];
};
