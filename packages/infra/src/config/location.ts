import { Context } from "effect";

/** Captured source and launch directories used only by infrastructure and the host. */
export class ConfigLocation extends Context.Service<
  ConfigLocation,
  {
    readonly baseDir: string;
    readonly projectRoot: string;
    readonly envPath: string;
    readonly homeDirectory: string;
  }
>()("infra/config/Location") {}
