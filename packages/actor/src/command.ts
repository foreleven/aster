import { Cause, Context, Effect, Option, Predicate, Schema } from "effect";
import { ReplyTo } from "./actor.js";

export const TypeId = "~aster/actor/Command" as const;

export interface Contract extends Schema.Top {
  readonly [TypeId]: true;
  readonly _tag: string;
  readonly description: string;
  readonly payloadSchema: Schema.Top;
  readonly replySchema: Schema.Top;
}

export const isContract = (schema: Schema.Top): schema is Contract =>
  Predicate.hasProperty(schema, TypeId) && schema[TypeId] === true;

type Options = {
  readonly payload: Schema.Struct.Fields;
  readonly description?: string;
} & (
  | { readonly reply: Schema.Top; readonly success?: never; readonly error?: never }
  | { readonly success: Schema.Top; readonly error?: Schema.Top; readonly reply?: never }
  | { readonly reply?: never; readonly success?: never; readonly error?: never }
);

const outcome = <S extends Schema.Top, E extends Schema.Top>(success: S, error: E) =>
  Schema.TaggedUnion({ Success: { value: success }, Failure: { error } });
type ResponseSchema<O extends Options> = O extends { readonly reply: infer R extends Schema.Top }
  ? R
  : O extends { readonly success: infer S extends Schema.Top }
    ? ReturnType<
        typeof outcome<
          S,
          O extends { readonly error: infer E extends Schema.Top } ? E : typeof Schema.Never
        >
      >
    : typeof Schema.Never;
type Response<O extends Options> = ResponseSchema<O>["Type"];
type Fields<O extends Options> = O["payload"] &
  (O extends { readonly reply: Schema.Top } | { readonly success: Schema.Top }
    ? { readonly replyTo: ReturnType<typeof ReplyTo<Response<O>>> }
    : unknown);
export type Reply<C extends Contract> = C["replySchema"]["Type"];

export type Outcome<A, E> =
  { readonly _tag: "Success"; readonly value: A } | { readonly _tag: "Failure"; readonly error: E };

/** Expected failures are replies; defects and interruption retain their Effect semantics. */
export const reply = <A, E, R>(replyTo: ReplyTo<Outcome<A, E>>, work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.matchCauseEffect({
      onSuccess: (value) => replyTo.tell({ _tag: "Success", value }),
      onFailure: (cause) => {
        // A mixed cause is an execution failure, never an ordinary error reply.
        // Preserve every reason; typed errors in a mixed cause become diagnostics.
        if (Cause.hasDies(cause) || Cause.hasInterrupts(cause))
          return Effect.failCause(
            Cause.fromReasons<never>(
              cause.reasons.map((reason) =>
                Cause.isFailReason(reason)
                  ? Cause.makeDieReason(reason.error).annotate(
                      Context.makeUnsafe(reason.annotations),
                    )
                  : reason,
              ),
            ),
          );
        return Option.match(Cause.findErrorOption(cause), {
          onSome: (error) => replyTo.tell({ _tag: "Failure", error }),
          onNone: () => Effect.failCause(Cause.empty),
        });
      },
    }),
  );

type Definition<Self, Tag extends string, O extends Options> = Schema.Class<
  Self,
  Schema.TaggedStruct<Tag, Fields<O>>,
  unknown
> & {
  readonly [TypeId]: true;
  readonly _tag: Tag;
  readonly description: string;
  readonly payloadSchema: Schema.Struct<O["payload"]>;
  readonly replySchema: ResponseSchema<O>;
};

/** A local message, an explicit reply protocol, or a typed success/error request.
 * The payload excludes the generated reply ref; only payload schemas cross transports.
 */
export const Class =
  <Self>() =>
  <const Tag extends string, const O extends Options>(
    tag: Tag,
    options: O,
  ): Definition<Self, Tag, O> => {
    const reply =
      options.reply ??
      (options.success ? outcome(options.success, options.error ?? Schema.Never) : Schema.Never);
    const fields =
      options.reply || options.success
        ? { ...options.payload, replyTo: ReplyTo<unknown>() }
        : options.payload;
    // Conditional fields follow the selected reply mode. The constructor validates those
    // same fields; this assertion only connects the generic option to its computed type.
    return Object.assign(Schema.TaggedClass<Self>()(tag, fields), {
      [TypeId]: true,
      _tag: tag,
      description: options.description ?? tag,
      payloadSchema: Schema.Struct(options.payload),
      replySchema: reply,
    }) as unknown as Definition<Self, Tag, O>;
  };
