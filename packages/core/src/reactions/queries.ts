import { Command } from "@aster/actor";
import { Schema } from "effect";

export class GetProcessing extends Command.Class<GetProcessing>()("GetProcessing", {
  payload: {},
  reply: Schema.Json,
}) {}
