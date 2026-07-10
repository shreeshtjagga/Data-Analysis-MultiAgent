import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    host: "0.0.0.0",
    proxy: {
      "/api": {
        target: process.env.VITE_API_BASE_URL || "http://localhost:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    target: "es2020",
    assetsInlineLimit: 4096,
    cssCodeSplit: true,
    // Use esbuild (built-in) instead of terser to avoid missing dep on Render
    minify: "esbuild",
    rollupOptions: {
      output: {
        manualChunks(id) {
          // Keep react and react-dom together — they have tight internal coupling
          if (
            id.includes("node_modules/react/") ||
            id.includes("node_modules/react-dom/")
          ) {
            return "react-core";
          }
          // Separate react-router to avoid circular dependency with react-dom
          if (id.includes("node_modules/react-router")) {
            return "react-router";
          }
          // Plotly is very large — isolate it
          if (id.includes("node_modules/plotly")) {
            return "plotly";
          }
          // Supabase auth
          if (
            id.includes("node_modules/@supabase") ||
            id.includes("node_modules/@react-oauth")
          ) {
            return "auth";
          }
          // Everything else in node_modules goes to vendor
          if (id.includes("node_modules/")) {
            return "vendor";
          }
        },
      },
    },
  },
});