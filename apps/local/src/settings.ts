import { Config } from "effect";

/** The HTTP adapter resolves its own settings during Layer acquisition. */
export const httpSettings = Config.all({
  port: Config.Port("port").pipe(Config.withDefault(4317), Config.nested("http")),
});
