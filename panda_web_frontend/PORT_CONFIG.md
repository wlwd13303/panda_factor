# 🔌 端口配置说明

## 后端服务端口

根据 `start_all.py` 配置：

| 服务 | 端口 | 说明 |
|-----|------|------|
| **Web服务** | **8080** | 前端静态文件 + 数据清洗API |
| **因子计算服务** | **8111** | 因子分析 + LLM聊天 |
| **React前端** | **3000** | 开发模式（Vite Dev Server） |

## API路径对应

### 数据清洗API (端口8080)
```
http://localhost:8080/datahub/api/v1/...
```
- 股票数据清洗
- 因子数据清洗
- 进度查询

### 因子计算API (端口8111)
```
http://localhost:8111/api/v1/...
或
http://localhost:8111/panda_factor/api/v1/...
```
- 因子列表管理
- 因子创建/编辑/删除
- 因子运行和分析

### LLM聊天API (端口8111)
```
http://localhost:8111/llm/...
```
- 聊天消息发送
- 会话管理

## Vite代理配置

开发模式下，Vite会自动代理API请求：

```typescript
// vite.config.ts
proxy: {
  '/datahub': {
    target: 'http://localhost:8080',  // 数据清洗服务
    changeOrigin: true,
  },
  '/api': {
    target: 'http://localhost:8111',  // 因子计算服务
    changeOrigin: true,
  },
  '/panda_factor': {
    target: 'http://localhost:8111',  // 因子计算服务（带前缀）
    changeOrigin: true,
  },
  '/llm': {
    target: 'http://localhost:8111',  // LLM聊天服务
    changeOrigin: true,
  },
}
```

## 环境变量

### 开发环境 (`.env.development`)
```bash
VITE_API_BASE_URL=http://localhost:8111
VITE_DATAHUB_BASE_URL=http://localhost:8080
VITE_FACTOR_API_BASE_URL=http://localhost:8111
VITE_LLM_API_BASE_URL=http://localhost:8111
```

### 生产环境 (`.env.production`)
```bash
VITE_API_BASE_URL=/api
VITE_DATAHUB_BASE_URL=/datahub
VITE_FACTOR_API_BASE_URL=/panda_factor
VITE_LLM_API_BASE_URL=/llm
```

## 启动顺序

1. **启动后端服务**
   ```bash
   python start_all.py
   ```
   这会启动：
   - Web服务 (8080)
   - 因子计算服务 (8111)
   - 数据自动更新任务

2. **启动React前端**
   ```bash
   python start_frontend.py
   # 或
   cd panda_web_frontend && npm run dev
   ```
   前端会在 **3000** 端口启动

3. **访问应用**
   ```
   http://localhost:3000
   ```

## 故障排查

### 检查端口占用

**Windows:**
```powershell
netstat -ano | findstr :8080
netstat -ano | findstr :8111
netstat -ano | findstr :3000
```

**Linux/Mac:**
```bash
lsof -i :8080
lsof -i :8111
lsof -i :3000
```

### 测试服务可用性

```bash
# 测试数据清洗服务
curl http://localhost:8080/datahub/api/v1/get_progress_stock_final

# 测试因子计算服务
curl http://localhost:8111/api/v1/hello

# 测试LLM服务（需要POST）
curl -X POST http://localhost:8111/llm/chat \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test","message":"hello"}'
```

## 修改端口

如果需要修改端口：

1. **修改后端端口**
   - 编辑 `start_all.py` 中的端口配置
   - 编辑各服务的启动脚本

2. **修改前端代理**
   - 编辑 `vite.config.ts`
   - 编辑 `.env.development`
   - 重启前端开发服务器

## 注意事项

⚠️ **重要**: 
- 因子计算服务 (8111) 同时提供因子API和LLM API
- 如果修改端口，需要同时更新前端配置
- 生产环境部署时，建议使用Nginx反向代理统一端口

