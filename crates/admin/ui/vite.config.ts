import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

// 单文件构建:JS/CSS 全部内联进一个 index.html,便于 Rust `include_str!` 内嵌(admin 单二进制部署)。
export default defineConfig({
  plugins: [react(), viteSingleFile()],
  build: {
    target: "es2020",
    assetsInlineLimit: 100000000,
    chunkSizeWarningLimit: 100000000,
    cssCodeSplit: false,
  },
});
