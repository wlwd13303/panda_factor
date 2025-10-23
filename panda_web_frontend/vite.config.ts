import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      // 代理所有 /api 请求到后端
      '/api': {
        target: 'http://localhost:8111',
        changeOrigin: true,
      },
      // 代理数据中心API
      '/datahub': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      // 代理因子服务API
      '/panda_factor': {
        target: 'http://localhost:8111',
        changeOrigin: true,
      },
      // 代理LLM服务API
      '/llm': {
        target: 'http://localhost:8111',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
          'antd-vendor': ['antd', '@ant-design/icons'],
        },
      },
    },
  },
})

