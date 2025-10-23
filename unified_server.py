"""
PandaAI 统一服务器
整合所有API服务到一个端口
"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
import base64
from pathlib import Path

# Import all routes
from panda_data_hub.routes.data_clean import factor_data_clean, stock_market_data_clean, financial_data_clean
from panda_data_hub.routes.config import config_redefine
from panda_data_hub.routes.query import data_query
from panda_factor_server.routes import user_factor_pro
from panda_llm.routes import chat_router

app = FastAPI(
    title="PandaAI 量化因子系统",
    description="统一API服务 - 整合数据清洗、因子计算、AI对话功能",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# 注册所有API路由
# ============================================================

# 数据清洗相关API
app.include_router(data_query.router, prefix="/datahub/api/v1", tags=["数据查询"])
app.include_router(config_redefine.router, prefix="/datahub/api/v1", tags=["配置管理"])
app.include_router(factor_data_clean.router, prefix="/datahub/api/v1", tags=["因子数据清洗"])
app.include_router(stock_market_data_clean.router, prefix="/datahub/api/v1", tags=["股票市场数据清洗"])
app.include_router(financial_data_clean.router, prefix="/datahub/api/v1", tags=["财务数据清洗"])

# 因子计算API
app.include_router(user_factor_pro.router, prefix="/api/v1", tags=["因子计算"])

# AI对话API
app.include_router(chat_router.router, prefix="/llm", tags=["AI对话"])

# ============================================================
# 静态文件服务 - 前端界面
# ============================================================

# 获取前端静态文件目录
frontend_dir = Path(__file__).parent / "panda_web" / "panda_web" / "static"

if frontend_dir.exists():
    # 挂载前端静态文件
    app.mount("/factor", StaticFiles(directory=str(frontend_dir), html=True), name="factor")
    # 兼容打包产物中对 "/assets/*" 的绝对路径请求
    assets_dir = frontend_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
    print(f"✓ 前端静态文件已挂载: {frontend_dir}")
else:
    print(f"⚠ 警告: 前端目录不存在: {frontend_dir}")

# 针对缺失资源提供兜底占位图，避免控制台 404 噪音
# 1x1 透明 PNG（base64）
_TRANSPARENT_PNG_BASE64 = (
    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Qm1cEwAAAAASUVORK5CYII="
)

@app.get("/assets/chat-dI4p2fsV.png")
async def _fallback_chat_png():
    try:
        file_path = (frontend_dir / "assets" / "chat-dI4p2fsV.png")
        if file_path.exists():
            return Response(content=file_path.read_bytes(), media_type="image/png")
    except Exception:
        pass
    return Response(content=base64.b64decode(_TRANSPARENT_PNG_BASE64), media_type="image/png")

# ============================================================
# 根路由 - 导航页面
# ============================================================

@app.get("/")
async def root():
    """根路径 - 返回服务信息"""
    return {
        "service": "PandaAI 量化因子系统",
        "version": "2.0.0",
        "endpoints": {
            "前端界面": "/factor/",
            "API文档": "/docs",
            "ReDoc文档": "/redoc",
            "数据清洗API": "/datahub/api/v1/",
            "因子计算API": "/api/v1/",
            "AI对话API": "/llm/"
        },
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy"}

# ============================================================
# 启动函数
# ============================================================

def main():
    import uvicorn
    from panda_common.logger_config import logger
    
    logger.info("=" * 60)
    logger.info("PandaAI 统一服务器启动中...")
    logger.info("=" * 60)
    
    # 显示服务信息
    print("\n" + "=" * 60)
    print("  PandaAI 量化因子系统 - 统一服务器")
    print("=" * 60)
    print("\n📊 服务地址:")
    print("  前端界面: http://localhost:8080/factor/")
    print("  API文档:  http://localhost:8080/docs")
    print("  健康检查: http://localhost:8080/health")
    print("\n🔌 API端点:")
    print("  数据清洗: /datahub/api/v1/")
    print("  因子计算: /api/v1/")
    print("  AI对话:   /llm/")
    print("\n⚡ 按 Ctrl+C 停止服务")
    print("=" * 60 + "\n")
    
    # 启动服务
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()

