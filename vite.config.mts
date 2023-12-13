/// <reference types="vitest" />
import { defineConfig } from "vite";
// import tsconfigPaths from "vite-tsconfig-paths";

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [
        // tsconfigPaths(),
    ],
    root: ".",
    test: {
        globals: true,
        environment: "node",
        coverage: {
            provider: "v8",
            reporter: "text",
        },
    },
});
