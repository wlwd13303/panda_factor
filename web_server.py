"""
天蝎座量化投资系统 Web服务器
整合前端 + 数据清洗API
不包含因子计算（避免初始化问题）
"""
import sys
from pathlib import Path

# Prefer local repo packages over site-packages to pick up latest changes.
repo_root = Path(__file__).parent.resolve()
local_pkg_root = repo_root / "panda_data_hub"
if local_pkg_root.exists():
    sys.path.insert(0, str(local_pkg_root))


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response, HTMLResponse
import base64

# Import data hub routes
from panda_data_hub.routes.data_clean import factor_data_clean, stock_market_data_clean, adj_factor_data_clean, financial_data_clean, dividend_data_clean, index_market_data_clean, valuation_factor_data_clean, namechange_data_clean, stock_info_data_clean
from panda_data_hub.routes.config import config_redefine
from panda_data_hub.routes.query import data_query

# Import AI chat routes
from panda_llm.routes import chat_router

app = FastAPI(
    title="天蝎座量化投资系统 Web服务",
    description="前端界面 + 数据清洗API",
    version="1.0.0"
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
# 注册API路由
# ============================================================

# 数据清洗相关API
app.include_router(data_query.router, prefix="/datahub/api/v1", tags=["数据查询"])
app.include_router(config_redefine.router, prefix="/datahub/api/v1", tags=["配置管理"])
app.include_router(factor_data_clean.router, prefix="/datahub/api/v1", tags=["因子数据清洗"])
app.include_router(stock_market_data_clean.router, prefix="/datahub/api/v1", tags=["股票市场数据清洗"])
app.include_router(adj_factor_data_clean.router, prefix="/datahub/api/v1", tags=["复权因子数据清洗"])
app.include_router(financial_data_clean.router, prefix="/datahub/api/v1", tags=["财务数据清洗"])
app.include_router(dividend_data_clean.router, prefix="/datahub/api/v1", tags=["分红数据清洗"])
app.include_router(index_market_data_clean.router, prefix="/datahub/api/v1", tags=["指数行情数据清洗"])
app.include_router(valuation_factor_data_clean.router, prefix="/datahub/api/v1", tags=["估值因子数据清洗"])
app.include_router(namechange_data_clean.router, prefix="/datahub/api/v1", tags=["股票名称清洗"])
app.include_router(stock_info_data_clean.router, prefix="/datahub/api/v1", tags=["股票基础信息清洗"])

# AI对话API
app.include_router(chat_router.router, prefix="/llm", tags=["AI对话"])

# ============================================================
# 静态文件服务 - 前端界面
# ============================================================

frontend_dir = Path(__file__).parent / "panda_web" / "panda_web" / "static"

if frontend_dir.exists():
    app.mount("/factor", StaticFiles(directory=str(frontend_dir), html=True), name="factor")
    # 兼容打包产物中对 "/assets/*" 的绝对路径请求
    assets_dir = frontend_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
    print(f"✓ 前端静态文件已挂载: {frontend_dir}")
else:
    print(f"⚠ 警告: 前端目录不存在: {frontend_dir}")

# Valuation Factor data cleaning page
@app.get("/valuation-factor-clean")
async def valuation_factor_clean_page():
    """估值因子数据清洗页面"""
    html_file = frontend_dir / "valuation_factor_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>估值因子数据清洗页面未找到</h1><p>请确保 valuation_factor_clean.html 文件存在</p>", status_code=404)

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
# 数据统计页面
# ============================================================

@app.get("/data-statistics")
async def data_statistics():
    """数据列表统计页面"""
    import os
    html_file = frontend_dir / "data_statistics.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>数据统计页面未找到</h1><p>请确保 data_statistics.html 文件存在</p>", status_code=404)

@app.get("/system-config")
async def system_config():
    """系统配置页面"""
    html_file = frontend_dir / "system_config.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>系统配置页面未找到</h1><p>请确保 system_config.html 文件存在</p>", status_code=404)

@app.get("/financial-data-clean")
async def financial_data_clean():
    """财务数据清洗页面"""
    html_file = frontend_dir / "financial_data_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>财务数据清洗页面未找到</h1><p>请确保 financial_data_clean.html 文件存在</p>", status_code=404)

@app.get("/stock-market-clean")
async def stock_market_clean():
    """股票行情清洗页面"""
    html_file = frontend_dir / "stock_market_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>股票行情清洗页面未找到</h1><p>请确保 stock_market_clean.html 文件存在</p>", status_code=404)

@app.get("/adj-factor-clean")
async def adj_factor_clean_page():
    """复权因子数据清洗页面"""
    html_file = frontend_dir / "adj_factor_data_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>复权因子数据清洗页面未找到</h1><p>请确保 adj_factor_data_clean.html 文件存在</p>", status_code=404)

@app.get("/factor-data-clean")
async def factor_data_clean_page():
    """因子数据清洗页面"""
    html_file = frontend_dir / "factor_data_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>因子数据清洗页面未找到</h1><p>请确保 factor_data_clean.html 文件存在</p>", status_code=404)

@app.get("/dividend-data-clean")
async def dividend_data_clean_page():
    """分红数据清洗页面"""
    html_file = frontend_dir / "dividend_data_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>分红数据清洗页面未找到</h1><p>请确保 dividend_data_clean.html 文件存在</p>", status_code=404)

@app.get("/index-market-clean")
async def index_market_clean_page():
    """指数行情清洗页面"""
    html_file = frontend_dir / "index_market_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>指数行情清洗页面未找到</h1><p>请确保 index_market_clean.html 文件存在</p>", status_code=404)

@app.get("/stock-name-clean")
async def stock_name_clean_page():
    """股票名称清洗页面"""
    html_file = frontend_dir / "stock_name_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>股票名称清洗页面未找到</h1><p>请确保 stock_name_clean.html 文件存在</p>", status_code=404)

@app.get("/stock-info-clean")
async def stock_info_clean_page():
    """股票基础信息清洗页面"""
    html_file = frontend_dir / "stock_info_clean.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="<h1>股票基础信息清洗页面未找到</h1><p>请确保 stock_info_clean.html 文件存在</p>", status_code=404)

# ============================================================
# 根路由
# ============================================================

@app.get("/")
async def navigation_home():
    """导航主页"""
    html_content = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>天蝎座量化投资系统 - 导航中心</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #333;
            }
            
            .container {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
                max-width: 800px;
                width: 90%;
            }
            
            .header {
                text-align: center;
                margin-bottom: 40px;
            }
            
            .logo {
                font-size: 2.5rem;
                font-weight: bold;
                background: linear-gradient(45deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 10px;
            }
            
            .subtitle {
                font-size: 1.1rem;
                color: #666;
                margin-bottom: 20px;
            }
            
            .nav-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .nav-item {
                background: linear-gradient(45deg, #f8f9fa, #e9ecef);
                border: 2px solid transparent;
                border-radius: 15px;
                padding: 25px;
                text-decoration: none;
                transition: all 0.3s ease;
                display: block;
                position: relative;
                overflow: hidden;
            }
            
            .nav-item::before {
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(45deg, #667eea, #764ba2);
                transition: left 0.5s ease;
                z-index: -1;
            }
            
            .nav-item:hover::before {
                left: 0;
            }
            
            .nav-item:hover {
                transform: translateY(-5px);
                box-shadow: 0 15px 30px rgba(0, 0, 0, 0.2);
                color: white;
            }
            
            .nav-title {
                font-size: 1.3rem;
                font-weight: bold;
                margin-bottom: 10px;
                display: flex;
                align-items: center;
            }
            
            .nav-icon {
                font-size: 1.5rem;
                margin-right: 10px;
            }
            
            .nav-desc {
                font-size: 0.95rem;
                opacity: 0.8;
                line-height: 1.4;
            }
            
            .status-bar {
                background: #f8f9fa;
                border-radius: 10px;
                padding: 15px;
                margin-top: 30px;
                border-left: 4px solid #28a745;
            }
            
            .status-title {
                font-weight: bold;
                color: #28a745;
                margin-bottom: 5px;
            }
            
            .status-info {
                font-size: 0.9rem;
                color: #666;
            }
            
            .footer {
                text-align: center;
                margin-top: 20px;
                color: #888;
                font-size: 0.9rem;
            }
            
            .service-status {
                display: inline-block;
                margin: 0 10px;
                padding: 2px 8px;
                border-radius: 10px;
                font-size: 0.85rem;
                font-weight: bold;
            }
            
            .service-running {
                background: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }
            
            .service-stopped {
                background: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }
            
            .service-checking {
                background: #fff3cd;
                color: #856404;
                border: 1px solid #ffeaa7;
            }
            
            .loading {
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
            /* 一键更新面板 */
            .action-panel {
                background: linear-gradient(135deg, #fef9e7 0%, #fdebd0 100%);
                border: 2px solid #f39c12;
                border-radius: 16px;
                padding: 24px;
                margin-bottom: 24px;
            }
            .action-header h3 {
                margin: 0 0 6px 0;
                color: #e67e22;
                font-size: 1.2rem;
            }
            .action-header p {
                margin: 0 0 16px 0;
                color: #b9770e;
                font-size: 0.85rem;
            }
            .one-click-btn {
                width: 100%;
                padding: 14px;
                font-size: 1.1rem;
                font-weight: bold;
                color: #fff;
                background: linear-gradient(135deg, #f39c12, #e67e22);
                border: none;
                border-radius: 10px;
                cursor: pointer;
                transition: all 0.3s ease;
                letter-spacing: 2px;
            }
            .one-click-btn:hover:not(:disabled) {
                transform: translateY(-2px);
                box-shadow: 0 8px 20px rgba(243, 156, 18, 0.4);
            }
            .one-click-btn:disabled {
                background: #ccc;
                cursor: not-allowed;
            }
            .task-list {
                margin-top: 16px;
            }
            .task-item {
                display: flex;
                align-items: center;
                padding: 10px 14px;
                margin-bottom: 6px;
                background: #fff;
                border-radius: 8px;
                font-size: 0.9rem;
                transition: all 0.3s ease;
            }
            .task-icon {
                width: 28px;
                height: 28px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin-right: 12px;
                font-size: 14px;
                flex-shrink: 0;
            }
            .task-waiting { background: #f0f0f0; color: #999; }
            .task-running { background: #fff3cd; color: #f39c12; animation: pulse 1.5s infinite; }
            .task-done { background: #d4edda; color: #27ae60; }
            .task-error { background: #f8d7da; color: #e74c3c; }
            .task-name { flex: 1; }
            .task-msg { font-size: 0.8rem; color: #888; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="logo">天蝎座</div>
                <div class="subtitle">量化投资系统 · 数据管理中心</div>
            </div>

            <!-- 一键更新面板 -->
            <div class="action-panel">
                <div class="action-header">
                    <h3>每日一键更新</h3>
                    <p>依次执行：基础信息 → 名称变更(30天) → 行情 → 复权因子 → 因子 → 估值因子(昨日)</p>
                </div>
                <button class="one-click-btn" id="oneClickBtn" onclick="startOneClickUpdate()">
                    开始一键更新
                </button>
                <div class="task-list" id="taskList"></div>
            </div>

            <div class="nav-grid">
                <a href="/factor/" class="nav-item">
                    <div class="nav-title">
                        因子管理主页
                    </div>
                    <div class="nav-desc">
                        查看因子列表、创建新因子、管理现有因子策略
                    </div>
                </a>
                
                <a href="/system-config" class="nav-item">
                    <div class="nav-title">
                        系统配置
                    </div>
                    <div class="nav-desc">
                        配置MongoDB、Tushare Token、数据清洗时间（配置立即生效）
                    </div>
                </a>
                
                <a href="/data-statistics" class="nav-item">
                    <div class="nav-title">
                        数据列表查看
                    </div>
                    <div class="nav-desc">
                        查看股票、因子、财务数据统计和数据完整性（支持财务数据）
                    </div>
                </a>
                
                <a href="/stock-info-clean" class="nav-item">
                    <div class="nav-title">
                        股票基础信息清洗
                    </div>
                    <div class="nav-desc">
                        更新股票、ETF、指数的基础列表信息（stock_info_new）
                    </div>
                </a>

                <a href="/stock-name-clean" class="nav-item">
                    <div class="nav-title">
                        股票名称清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票名称变更历史数据（支持全量和增量清洗，按个股查询）
                    </div>
                </a>

                <a href="/stock-market-clean" class="nav-item">
                    <div class="nav-title">
                        股票行情清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票日线行情数据（开高低收、成交量、成交额等）
                    </div>
                </a>

                <a href="/adj-factor-clean" class="nav-item">
                    <div class="nav-title">
                        复权因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗复权因子数据，用于精确计算前/后复权价格（前复权 = 不复权价 × adj_factor / 最新adj_factor）
                    </div>
                </a>

                
                <a href="/factor-data-clean" class="nav-item">
                    <div class="nav-title">
                        因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗基础市场因子（市值、换手率、成交额等）
                    </div>
                </a>
                
                <a href="/financial-data-clean" class="nav-item">
                    <div class="nav-title">
                        财务数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗财务指标、利润表、资产负债表、现金流量表
                    </div>
                </a>
                
                <a href="/dividend-data-clean" class="nav-item">
                    <div class="nav-title">
                        分红数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票分红数据（送股、转增、现金分红等）
                    </div>
                </a>
                
                <a href="/index-market-clean" class="nav-item">
                    <div class="nav-title">
                        指数行情清洗
                    </div>
                    <div class="nav-desc">
                        清洗主要指数日线行情（上证、深证、沪深300、中证500等）
                    </div>
                </a>
                
                <a href="/valuation-factor-clean" class="nav-item">
                    <div class="nav-title">
                        估值因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗估值因子数据（PE-TTM、市销率、市现率、流通市值）
                    </div>
                </a>
            </div>
            
            <div class="status-bar">
                <div class="status-title" style="cursor: pointer;" title="点击刷新状态">系统状态 🔄</div>
                <div class="status-info" id="systemStatus">
                    <span class="loading">正在检测服务状态...</span>
                </div>
            </div>
            
            <div class="footer">
                天蝎座量化投资系统 © 2025 | 天蝎座私募基金公司
            </div>
        </div>
        
        <script>
            // 添加一些交互效果
            document.querySelectorAll('.nav-item').forEach(item => {
                item.addEventListener('click', function(e) {
                    // 添加点击效果
                    this.style.transform = 'scale(0.95)';
                    setTimeout(() => {
                        this.style.transform = 'translateY(-5px)';
                    }, 100);
                });
            });
            
            // 检查服务状态
            async function checkServiceStatus(port, name) {
                try {
                    const response = await fetch(`http://localhost:${port}/health`, {
                        method: 'GET',
                        mode: 'cors'
                    });
                    if (response.ok) {
                        return {
                            name: name,
                            port: port,
                            status: 'running',
                            message: '运行中'
                        };
                    } else {
                        throw new Error('Service not healthy');
                    }
                } catch (error) {
    return {
                        name: name,
                        port: port,
                        status: 'stopped',
                        message: '未启动'
                    };
                }
            }
            
            async function updateSystemStatus() {
                const statusElement = document.getElementById('systemStatus');
                statusElement.innerHTML = '<span class="loading">正在检测服务状态...</span>';
                
                try {
                    // 检查三个服务的状态
                    const services = [
                        { port: 19080, name: '前端服务' },
                        { port: 19111, name: '因子计算服务' }
                    ];
                    
                    const statusPromises = services.map(service => 
                        checkServiceStatus(service.port, service.name)
                    );
                    
                    const results = await Promise.all(statusPromises);
                    
                    // 构建状态显示
                    let statusHTML = '';
                    results.forEach(result => {
                        const statusClass = result.status === 'running' ? 'service-running' : 'service-stopped';
                        const statusIcon = result.status === 'running' ? '✅' : '❌';
                        statusHTML += `<span class="service-status ${statusClass}">${statusIcon} ${result.name}：${result.message} (${result.port}端口)</span>`;
                    });
                    
                    statusElement.innerHTML = statusHTML;
                    
                } catch (error) {
                    statusElement.innerHTML = '<span class="service-checking">服务状态检测失败</span>';
                }
            }
            
            // 页面加载完成后检查状态
            document.addEventListener('DOMContentLoaded', function() {
                console.log('Scorpio Quant System Navigation Center Loaded');
                updateSystemStatus();
                
                // 每30秒自动检查一次服务状态
                setInterval(updateSystemStatus, 30000);
            });
            
            // 一键更新逻辑
            const TASKS = [
                {
                    name: '股票基础信息清洗',
                    trigger: () => fetch('/datahub/api/v1/stock_info_clean'),
                    progressUrl: '/datahub/api/v1/get_progress_stock_info'
                },
                {
                    name: '股票名称清洗(近30天)',
                    trigger: () => {
                        const today = new Date();
                        const d30 = new Date(today); d30.setDate(today.getDate() - 30);
                        const fmt = d => d.toISOString().split('T')[0].replace(/-/g, '');
                        return fetch(`/datahub/api/v1/upsert_namechange_data?start_date=${fmt(d30)}&end_date=${fmt(today)}&force_update=false`);
                    },
                    progressUrl: '/datahub/api/v1/get_progress_namechange'
                },
                {
                    name: '股票行情清洗(昨日)',
                    trigger: () => {
                        const d = new Date(); d.setDate(d.getDate() - 1);
                        const y = d.toISOString().split('T')[0].replace(/-/g, '');
                        return fetch(`/datahub/api/v1/upsert_stockmarket_final?start_date=${y}&end_date=${y}&force_update=false`);
                    },
                    progressUrl: '/datahub/api/v1/get_progress_stockmarket_final'
                },
                {
                    name: '复权因子清洗(昨日)',
                    trigger: () => {
                        const d = new Date(); d.setDate(d.getDate() - 1);
                        const y = d.toISOString().split('T')[0].replace(/-/g, '');
                        return fetch(`/datahub/api/v1/upsert_adj_factor_final?start_date=${y}&end_date=${y}&force_update=false`);
                    },
                    progressUrl: '/datahub/api/v1/get_progress_adj_factor_final'
                },
                {
                    name: '因子数据清洗(昨日)',
                    trigger: () => {
                        const d = new Date(); d.setDate(d.getDate() - 1);
                        const y = d.toISOString().split('T')[0].replace(/-/g, '');
                        return fetch(`/datahub/api/v1/upsert_factor_final?start_date=${y}&end_date=${y}`);
                    },
                    progressUrl: '/datahub/api/v1/get_progress_factor_final'
                },
                {
                    name: '估值因子清洗(昨日)',
                    trigger: () => {
                        const d = new Date(); d.setDate(d.getDate() - 1);
                        const y = d.toISOString().split('T')[0];
                        return fetch(`/datahub/api/v1/upsert_valuation_factor?start_date=${y}&end_date=${y}`);
                    },
                    progressUrl: '/datahub/api/v1/get_progress_valuation_factor'
                }
            ];

            function renderTasks() {
                const list = document.getElementById('taskList');
                list.innerHTML = TASKS.map((t, i) =>
                    `<div class="task-item" id="task-${i}">
                        <div class="task-icon task-waiting" id="icon-${i}">-</div>
                        <span class="task-name">${t.name}</span>
                        <span class="task-msg" id="msg-${i}"></span>
                    </div>`
                ).join('');
            }

            function setTaskState(i, state, msg) {
                const icon = document.getElementById('icon-' + i);
                const msgEl = document.getElementById('msg-' + i);
                if (icon) {
                    icon.className = 'task-icon task-' + state;
                    icon.textContent = { waiting: '-', running: '⏳', done: '✓', error: '✗' }[state] || '-';
                }
                if (msgEl) msgEl.textContent = msg || '';
            }

            async function waitForTask(triggerFn, progressUrl, taskIdx) {
                const resp = await triggerFn();
                if (!resp.ok) throw new Error('启动失败: HTTP ' + resp.status);

                const maxWait = 30 * 60 * 1000; // 30 min timeout
                const start = Date.now();
                while (Date.now() - start < maxWait) {
                    await new Promise(r => setTimeout(r, 2000));
                    try {
                        const pr = await fetch(progressUrl);
                        const data = await pr.json();
                        if (data.status === 'completed') return;
                        if (data.status === 'error') throw new Error(data.error_message || '任务执行失败');
                        setTaskState(taskIdx, 'running', data.current_task || data.progress_percent + '%');
                    } catch (e) {
                        if (e.message && e.message.includes('任务')) throw e;
                    }
                }
                throw new Error('任务超时(30分钟)');
            }

            async function startOneClickUpdate() {
                const btn = document.getElementById('oneClickBtn');
                btn.disabled = true;
                btn.textContent = '更新中...';
                renderTasks();

                for (let i = 0; i < TASKS.length; i++) {
                    setTaskState(i, 'running', '启动中...');
                    try {
                        await waitForTask(TASKS[i].trigger, TASKS[i].progressUrl, i);
                        setTaskState(i, 'done', '完成');
                    } catch (e) {
                        setTaskState(i, 'error', e.message);
                        btn.disabled = false;
                        btn.textContent = '开始一键更新';
                        return;
                    }
                }

                btn.disabled = false;
                btn.textContent = '开始一键更新';
            }

            // 添加手动刷新状态的功能
            document.querySelector('.status-title').addEventListener('click', function() {
                updateSystemStatus();
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "service": "web"}

@app.get("/api/info")
async def api_info():
    """API信息（JSON格式）"""
    return {
        "service": "天蝎座量化投资系统 Web服务",
        "version": "1.0.0",
        "endpoints": {
            "前端界面": "/factor/",
            "API文档": "/docs",
            "数据清洗API": "/datahub/api/v1/",
            "AI对话API": "/llm/"
        },
        "status": "running",
        "note": "因子计算服务运行在 19111 端口"
    }

# ============================================================
# 启动函数
# ============================================================

def main():
    import uvicorn
    from panda_common.logger_config import logger
    from panda_data_hub.utils.init_app import init_app

    logger.info("=" * 60)
    logger.info("天蝎座量化投资系统 Web服务器启动中...")
    logger.info("=" * 60)
    
    # 初始化应用（包括 tushare 连接）
    init_app()

    print("\n" + "=" * 60)
    print("  天蝎座量化投资系统 Web服务器")
    print("=" * 60)
    print("\n服务地址:")
    print("  前端界面: http://localhost:19080/factor/")
    print("  API文档:  http://localhost:19080/docs")
    print("  健康检查: http://localhost:19080/health")
    print("\n🔌 API端点:")
    print("  数据清洗: /datahub/api/v1/")
    print("  配置管理: /datahub/api/v1/config_redefine_data_source")
    print("  AI对话:   /llm/")
    print("\n💡 提示:")
    print("  - 此服务包含前端界面、数据清洗和AI对话功能")
    print("  - 因子计算服务需要单独启动（端口19111）")
    print("  - AI对话功能已集成")
    print("\n⚡ 按 Ctrl+C 停止服务")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=19080,
        log_level="info"
    )

if __name__ == "__main__":
    main()

