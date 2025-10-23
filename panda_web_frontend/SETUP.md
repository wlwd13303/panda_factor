# 安装和运行指南

## 前提条件

- Node.js 18+ 
- npm 或 pnpm

## 快速开始

### 1. 安装依赖

```bash
cd panda_web_frontend
npm install
# 或者使用 pnpm
pnpm install
```

### 2. 启动开发服务器

```bash
npm run dev
```

访问: http://localhost:3000

### 3. 构建生产版本

```bash
npm run build
```

构建产物在 `dist/` 目录

### 4. 预览生产构建

```bash
npm run preview
```

## 开发指南

### 目录结构

```
src/
├── api/              # API请求封装
│   ├── axios.ts     # Axios实例配置
│   ├── dataClean.ts # 数据清洗API
│   └── factor.ts    # 因子分析API
├── components/       # 通用组件
│   ├── Layout/      # 主布局
│   └── ProgressPanel/ # 进度面板
├── pages/           # 页面组件
│   ├── DataClean/   # 数据清洗页
│   └── FactorAnalysis/ # 因子分析页
├── routes/          # 路由配置
├── types/           # TypeScript类型
├── utils/           # 工具函数
├── App.tsx          # 根组件
└── main.tsx         # 入口文件
```

### API代理配置

在开发模式下，Vite会自动代理以下路径：

- `/api/*` → `http://localhost:8080`
- `/datahub/*` → `http://localhost:8080`
- `/panda_factor/*` → `http://localhost:8765`

如需修改代理配置，编辑 `vite.config.ts`

### 添加新页面

1. 在 `src/pages/` 创建新目录和组件
2. 在 `src/routes/index.tsx` 添加路由
3. 在 `src/components/Layout/index.tsx` 添加菜单项

### 添加新API

1. 在 `src/types/index.ts` 定义类型
2. 在 `src/api/` 创建API文件
3. 使用统一的 `axios` 实例

## 常见问题

### 1. 端口冲突

修改 `vite.config.ts` 中的 `server.port`

### 2. API请求失败

- 检查后端服务是否启动
- 检查代理配置是否正确
- 查看浏览器控制台Network标签

### 3. TypeScript错误

运行类型检查：
```bash
npm run type-check
```

## 部署

### 方法1: 静态文件部署

1. 构建项目: `npm run build`
2. 将 `dist/` 目录部署到Web服务器
3. 配置Nginx反向代理后端API

### 方法2: 与现有Python服务集成

将构建后的文件复制到 `panda_web/panda_web/static/`:

```bash
npm run build
rm -rf ../panda_web/panda_web/static/*
cp -r dist/* ../panda_web/panda_web/static/
```

## 性能优化

- 生产构建自动启用代码分割
- 使用 React.lazy() 进行路由懒加载
- Ant Design 组件按需引入

## 更多信息

- [Vite 文档](https://vitejs.dev/)
- [React 文档](https://react.dev/)
- [Ant Design 文档](https://ant.design/)

