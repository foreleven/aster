import js from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import prettier from "eslint-config-prettier/flat";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import reactX from "eslint-plugin-react-x";
import globals from "globals";
import tseslint from "typescript-eslint";

export default defineConfig([
  globalIgnores([
    "repos/**",
    "**/node_modules/**",
    "**/dist/**",
    "**/.test-dist/**",
    "**/coverage/**",
    "**/test-results/**",
    "**/playwright-report/**",
    "**/.aster/**",
    "**/.signals/**",
  ]),
  {
    files: ["**/*.{js,mjs,cjs,jsx,ts,tsx}"],
    extends: [js.configs.recommended],
    languageOptions: { globals: globals.node },
    linterOptions: { reportUnusedDisableDirectives: "error" },
  },
  {
    files: ["**/*.{ts,tsx}", "apps/web/src/**/*.{js,jsx}"],
    extends: [tseslint.configs.recommended],
    rules: {
      // Effect Actor protocols and external SDK adapters use intentional erased types.
      // Strict TypeScript checking remains the authority for those boundaries.
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrors: "none" },
      ],
    },
  },
  {
    files: ["apps/web/src/**/*.{js,jsx}"],
    languageOptions: {
      globals: globals.browser,
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    plugins: { "react-x": reactX, "react-hooks": reactHooks },
    rules: {
      "react-x/no-missing-key": "error",
      "react-x/no-duplicate-key": "error",
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "error",
    },
  },
  {
    files: ["apps/web/src/**/*.jsx"],
    extends: [reactRefresh.configs.vite],
  },
  {
    files: ["apps/web/test/**/*.js"],
    languageOptions: { globals: globals.browser },
  },
  // Formatting belongs to Prettier, not ESLint's stylistic rules.
  prettier,
]);
