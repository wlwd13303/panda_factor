# Panda Factor Web Frontend

基于 React + TypeScript + Vite + Ant Design 的现代化前端项目

## 技术栈

- **React 18** - UI框架
- **TypeScript** - 类型安全
- **Vite** - 快速构建工具
- **Ant Design 5** - UI组件库
- **React Router 6** - 路由管理
- **Zustand** - 轻量级状态管理
- **Axios** - HTTP客户端

## 开发

```bash
# 安装依赖
npm install

# 启动开发服务器 (http://localhost:3000)
npm run dev

# 构建生产版本
npm run build

# 预览构建结果
npm run preview
```

## 项目结构

```
src/
├── api/              # API请求
├── components/       # 通用组件
├── pages/           # 页面组件
├── routes/          # 路由配置
├── store/           # 状态管理
├── types/           # TypeScript类型定义
├── utils/           # 工具函数
├── App.tsx          # 根组件
└── main.tsx         # 入口文件
```

## API代理

开发环境会自动代理以下路径到后端：
- `/api/*` → `http://localhost:8080`
- `/datahub/*` → `http://localhost:8080`
- `/panda_factor/*` → `http://localhost:8765`

