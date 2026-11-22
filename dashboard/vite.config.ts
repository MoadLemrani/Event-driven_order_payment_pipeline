import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
export default defineConfig({ plugins: [react(), tailwindcss()], server: { proxy: {
  '/api/order': { target: 'http://localhost:8081', rewrite: p => p.replace(/^\/api\/order/, '') },
  '/api/inventory': { target: 'http://localhost:8082', rewrite: p => p.replace(/^\/api\/inventory/, '') },
  '/api/payment': { target: 'http://localhost:8083', rewrite: p => p.replace(/^\/api\/payment/, '') },
} } })
