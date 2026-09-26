import { createRequire } from "node:module";
import { installJsdomEnvironmentAdapter } from "../jsdom-compat.mts";

// Package-local Vitest workers can use a different installation than this preload.
const require = createRequire(process.argv[1]!);
const { builtinEnvironments }: typeof import("vitest/runtime") = require("vitest/runtime");
installJsdomEnvironmentAdapter(builtinEnvironments.jsdom);
