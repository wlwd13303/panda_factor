# 快速开始指南

## 🚀 5分钟启动项目

### 步骤1: 创建环境变量文件

在 `panda_web_frontend/` 目录下创建两个文件：

**`.env.development`**:
```
VITE_API_BASE_URL=http://localhost:8080
VITE_FACTOR_API_BASE_URL=http://localhost:8765
```

**`.env.production`**:
```
VITE_API_BASE_URL=/api
VITE_FACTOR_API_BASE_URL=/panda_factor
```

### 步骤2: 安装依赖

```bash
cd panda_web_frontend
npm install
```

> 💡 如果安装很慢，可以使用国内镜像：
> ```bash
> npm config set registry https://registry.npmmirror.com
> ```

### 步骤3: 启动开发服务器

```bash
npm run dev
```

或者使用根目录的启动脚本：

```bash
cd ..
python start_frontend.py
```

### 步骤4: 访问应用

打开浏览器访问: **http://localhost:3000**

## ⚠️ 注意事项

1. **确保后端服务已启动**
   - 数据中心服务: `http://localhost:8080`
   - 因子分析服务: `http://localhost:8765`

2. **Node.js版本要求**
   - 需要 Node.js 18 或更高版本
   - 检查版本: `node --version`

3. **端口冲突**
   - 如果3000端口被占用，Vite会自动使用下一个可用端口
   - 或者修改 `vite.config.ts` 中的 `server.port`

## 📦 常用命令

| 命令 | 说明 |
|-----|-----|
| `npm run dev` | 启动开发服务器 |
| `npm run build` | 构建生产版本 |
| `npm run preview` | 预览生产构建 |
| `npm run lint` | 代码检查 |

## 🎯 下一步

1. 查看 [README.md](./README.md) 了解技术栈
2. 查看 [SETUP.md](./SETUP.md) 了解详细配置
3. 开始开发新功能！

## 🐛 遇到问题？

1. **依赖安装失败**: 尝试删除 `node_modules` 和 `package-lock.json` 后重新安装
2. **启动失败**: 检查Node.js版本和端口占用
3. **API请求失败**: 确认后端服务已启动
4. **页面空白**: 打开浏览器控制台查看错误信息

