import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { parse } from "yaml";
import { parseConfig } from "@aster/core";

export const loadConfig = (path: string) =>
  parseConfig(parse(readFileSync(resolve(path), "utf8")), dirname(resolve(path)));
