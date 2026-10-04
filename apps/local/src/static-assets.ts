import { Effect, FileSystem } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { resolve, extname, sep } from "node:path";
import { ApplicationError } from "@aster/api-contracts";
import { json } from "./http-policy.js";

const contentTypes: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".svg": "image/svg+xml",
};
export const staticAssets = (webDir: string | undefined, url: string) =>
  HttpRouter.addAll([
    HttpRouter.route(
      "GET",
      "/*",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const path = new URL(request.url, url).pathname;
        if (!webDir || path.startsWith("/api/")) return json({ error: "Not found" }, 404);
        const root = resolve(webDir);
        const decoded = yield* Effect.try({
          try: () => decodeURIComponent(path),
          catch: () => new ApplicationError({ kind: "invalid-input", message: "Invalid path" }),
        });
        const file = resolve(root, `.${decoded === "/" ? "/index.html" : decoded}`);
        if (!file.startsWith(root + sep)) return json({ error: "Invalid path" }, 403);
        const fs = yield* FileSystem.FileSystem;
        return yield* fs.readFile(file).pipe(
          Effect.map((data) =>
            HttpServerResponse.uint8Array(data, {
              contentType: contentTypes[extname(file)] ?? "application/octet-stream",
              headers: { "x-content-type-options": "nosniff" },
            }),
          ),
          Effect.catchTag("PlatformError", () => Effect.succeed(json({ error: "Not found" }, 404))),
        );
      }),
    ),
  ]);
