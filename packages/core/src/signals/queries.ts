import { Effect, Schema } from "effect";
import { SignalSnapshot } from "./state/snapshot.js";

import { makeCollectionQueries } from "../context/queries/commands.js";
import { publicJson } from "../json.js";
export const makeSignalQueries = () =>
  makeCollectionQueries(
    "/signals",
    Effect.fnUntraced(function* (record, detail) {
      const state = yield* Schema.decodeUnknownEffect(SignalSnapshot)(record.state).pipe(
        Effect.orDie,
      );
      return publicJson({
        path: record.path,
        owner: state.owner,
        status: state.status,
        trigger: state.trigger,
        nextDue: state.nextDue,
        ...(detail ? { task: state.task, version: state.version } : {}),
      });
    }),
  );
