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
    minify: "esbuild",
    // Let Vite handle chunking automatically — manual chunks were causing
    // React to be loaded from two different bundles, crashing the app.
    chunkSizeWarningLimit: 12000,
  },
});