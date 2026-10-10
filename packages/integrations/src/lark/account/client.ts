import { Context, Effect, Layer, Schema } from "effect";
import { LarkAccountError } from "../errors.js";
import { decodeCliOutput, LarkCli } from "../cli.js";
import { AccountProfile } from "./model.js";

const User = Schema.Struct({
  open_id: Schema.NonEmptyString,
  name: Schema.String,
  email: Schema.optional(Schema.String),
  enterprise_email: Schema.optional(Schema.String),
});
const Response = Schema.Union([Schema.Struct({ user: User }), User]);
export class LarkAccountClient extends Context.Service<
  LarkAccountClient,
  {
    readonly getAccount: () => Effect.Effect<AccountProfile, LarkAccountError>;
  }
>()("lark/AccountClient") {
  static readonly layer = Layer.effect(
    LarkAccountClient,
    Effect.map(LarkCli, (cli) => makeAccountClient(cli)),
  );
}
export const makeAccountClient = (cli: LarkCli["Service"]): LarkAccountClient["Service"] => ({
  getAccount: Effect.fn("LarkAccountClient.getAccount")(
    function* () {
      const data = yield* cli
        .run(["contact", "+get-user", "--as", "user"])
        .pipe(Effect.flatMap(decodeCliOutput));
      const response = yield* Schema.decodeUnknownEffect(Response)(data);
      const user = "user" in response ? response.user : response;
      return {
        openId: user.open_id,
        name: user.name,
        email: user.email ?? "",
        enterpriseEmail: user.enterprise_email ?? "",
      };
    },
    Effect.mapError(
      (cause) => new LarkAccountError({ message: "Cannot load Lark account", cause }),
    ),
  ),
});
