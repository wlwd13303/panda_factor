import os
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse

app = FastAPI(title="PandaAI Web Interface")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get the absolute path to the static directory
DIST_DIR = os.path.join(os.path.dirname(__file__), "static")

# Mount the Vue dist directory at /factor path
app.mount("/factor", StaticFiles(directory=DIST_DIR, html=True), name="static")

# Create a beautiful navigation homepage
@app.get("/")
async def navigation_home():
    html_content = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PandaAI 量化因子系统 - 导航中心</title>
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
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="logo">🐼 PandaAI</div>
                <div class="subtitle">量化因子系统 · 数据管理中心</div>
            </div>
            
            <div class="nav-grid">
                <a href="/factor/" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">🏠</span>
                        因子管理主页
                    </div>
                    <div class="nav-desc">
                        查看因子列表、创建新因子、管理现有因子策略
                    </div>
                </a>
                
                <a href="/factor/#/datahubsource" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">⚙️</span>
                        数据源配置
                    </div>
                    <div class="nav-desc">
                        配置 Tushare、米筐、迅投等数据源的API密钥和参数
                    </div>
                </a>
                
                <a href="/factor/#/datahublist" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📊</span>
                        数据列表查看
                    </div>
                    <div class="nav-desc">
                        查看数据清洗状态、交易日数据统计和数据完整性
                    </div>
                </a>
                
                <a href="/factor/#/datahubdataclean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">🧹</span>
                        股票数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票行情数据、成交量数据和基础信息
                    </div>
                </a>
                
                <a href="/factor/#/datahubFactorClean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">🔧</span>
                        因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗技术指标、财务数据和自定义因子数据
                    </div>
                </a>
            </div>
            
            <div class="status-bar">
                <div class="status-title" style="cursor: pointer;" title="点击刷新状态">✅ 系统状态 🔄</div>
                <div class="status-info" id="systemStatus">
                    <span class="loading">正在检测服务状态...</span>
                </div>
            </div>
            
            <div class="footer">
                PandaAI 量化因子库 © 2025 | 让每个Alpha都被看见
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
                    const response = await fetch(`http://localhost:${port}/`, {
                        method: 'GET',
                        mode: 'cors',
                        timeout: 3000
                    });
                    return {
                        name: name,
                        port: port,
                        status: 'running',
                        message: '运行中'
                    };
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
                        { port: 8080, name: '前端服务' },
                        { port: 8111, name: '后端API' },
                        { port: 8222, name: '数据服务' }
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
                console.log('PandaAI Navigation Center Loaded');
                updateSystemStatus();
                
                // 每30秒自动检查一次服务状态
                setInterval(updateSystemStatus, 30000);
            });
            
            // 添加手动刷新状态的功能
            document.querySelector('.status-title').addEventListener('click', function() {
                updateSystemStatus();
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True  # Enable auto-reload during development
    ) 