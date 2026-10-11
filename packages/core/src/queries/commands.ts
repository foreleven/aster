import { Command } from "@aster/actor";
import { Schema } from "effect";

/** Opt in to discovery while retaining the command's own success/error protocol. */
export const ContextCommand = {
  Class:
    <Self>() =>
    <
      const Tag extends string,
      const F extends Schema.Struct.Fields,
      S extends Schema.Codec<unknown, unknown>,
      E extends Schema.Codec<unknown, unknown> = typeof Schema.Never,
    >(
      tag: Tag,
      options: {
        readonly payload: F;
        readonly description: string;
        readonly success: S;
        readonly error?: E;
      },
    ) =>
      Object.assign(
        Command.Class<Self>()(tag, {
          payload: options.payload,
          description: options.description,
          success: options.success,
          error: options.error ?? Schema.Never,
        }),
        {
          contextQuery: true as const,
          successSchema: options.success,
          errorSchema: options.error ?? Schema.Never,
          /** Override on the command class to present successful evidence to Agents. */
          text: (value: S["Type"]): string =>
            JSON.stringify(Schema.encodeSync(options.success)(value)) ?? "",
        },
      ),
};

/** The catalogue erases response types; typed Actor asks retain each class's protocol. */
export interface QueryCommand extends Command.Contract {
  readonly contextQuery: true;
  readonly payloadSchema: Schema.Codec<unknown, unknown>;
  readonly successSchema: Schema.Codec<unknown, unknown>;
  readonly errorSchema: Schema.Codec<unknown, unknown>;
  text(value: unknown): string;
  readonly DecodingServices: never;
  readonly EncodingServices: never;
}
export const isQueryCommand = (schema: Schema.Top): schema is QueryCommand =>
  Command.isContract(schema) && "contextQuery" in schema && schema.contextQuery === true;

/** Internal correlation only; this is never a command's declared response schema. */
export const QueryReply = Schema.TaggedUnion({
  Success: { value: Schema.Unknown },
  Failure: { error: Schema.Unknown },
});
