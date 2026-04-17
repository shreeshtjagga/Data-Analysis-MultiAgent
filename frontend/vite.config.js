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
    // Target modern browsers — skips legacy polyfills, smaller output
    target: "es2020",
    // Inline assets < 4 KB directly into HTML (saves network round trips)
    assetsInlineLimit: 4096,
    cssCodeSplit: true,
    minify: "terser",
    terserOptions: {
      compress: {
        drop_console: true,   // strip console.log in production builds
        drop_debugger: true,
        passes: 2,
      },
    },
    rollupOptions: {
      output: {
        // Granular chunks so browsers cache each library independently
        manualChunks(id) {
          if (
            id.includes("node_modules/react/") ||
            id.includes("node_modules/react-dom/") ||
            id.includes("node_modules/react-router-dom/")
          ) {
            return "react-core";
          }
          if (id.includes("node_modules/plotly")) {
            return "plotly";
          }
          if (
            id.includes("node_modules/three") ||
            id.includes("node_modules/@react-three")
          ) {
            return "three";
          }
          if (
            id.includes("node_modules/@react-oauth") ||
            id.includes("node_modules/jwt-decode")
          ) {
            return "auth";
          }
          if (id.includes("node_modules/")) {
            return "vendor";
          }
        },
      },
    },
  },
});