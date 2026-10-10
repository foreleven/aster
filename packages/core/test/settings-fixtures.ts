import { Layer } from "effect";
import { LanguageSettings, ContextDirectory, ReactionSettings } from "../src/index.js";

export const testSettings = Layer.mergeAll(
  LanguageSettings.layer,
  ContextDirectory.layer,
  ReactionSettings.layer,
);
