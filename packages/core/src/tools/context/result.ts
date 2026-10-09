import { Schema } from "effect";

const Page = Schema.Struct({
  resultId: Schema.Int,
  path: Schema.String,
  content: Schema.String,
  totalCharacters: Schema.Int,
  nextOffset: Schema.NullOr(Schema.Int),
});

/** Command text stays readable; paging identifiers remain available to the Agent. */
export const queryPageOutput = (value: unknown) => {
  const page = Schema.decodeUnknownSync(Page)(value);
  const cursor = page.nextOffset === null ? "complete" : `nextOffset: ${page.nextOffset}`;
  return {
    content: [
      {
        type: "text" as const,
        text: `Result ${page.resultId} (${page.path}; ${cursor})\n\n${page.content}`,
      },
    ],
    details: page,
  };
};
