import eslint from "@eslint/js";
import drizzle from "eslint-plugin-drizzle";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig(
  {
    ignores: ["dist/**", "node_modules/**"],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  {
    languageOptions: {
      globals: {
        structuredClone: "readonly",
      },
    },
  },
  {
    files: ["src/**/*.ts"],
    plugins: {
      drizzle,
    },
    rules: {
      "@typescript-eslint/ban-ts-comment": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unsafe-function-type": "off",
      "@typescript-eslint/no-extraneous-class": "off",
      "@typescript-eslint/no-useless-constructor": "off",
      "@typescript-eslint/unified-signatures": "off",
      "@typescript-eslint/no-unused-vars": ["error", { caughtErrors: "none" }],
      "drizzle/enforce-delete-with-where": [
        "error",
        { drizzleObjectName: ["db", "database", "_database"] },
      ],
      "drizzle/enforce-update-with-where": [
        "error",
        { drizzleObjectName: ["db", "database", "_database"] },
      ],
    },
  },
  {
    files: ["test/**/*.js"],
    rules: {
      "@typescript-eslint/no-this-alias": "off",
    },
  },
);
