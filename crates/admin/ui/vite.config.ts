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
  server: {
    // dev 时把 /api 与 /metrics 代理到已在跑的 admin 后端(默认本机 7300,可用 ANTFS_ADMIN_TARGET 覆盖),
    // 这样 `npm run dev` 不需要 CORS、也不用手改前端里的相对路径。
    proxy: {
      "/api": process.env.ANTFS_ADMIN_TARGET ?? "http://127.0.0.1:7300",
      "/metrics": process.env.ANTFS_ADMIN_TARGET ?? "http://127.0.0.1:7300",
    },
  },
});
