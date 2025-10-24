#!/usr/bin/env python3
"""
前端开发服务器启动脚本
"""
import os
import subprocess
import sys
from pathlib import Path
import shutil

def check_npm():
    """检查npm是否可用"""
    npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
    
    # 首先检查npm是否在PATH中
    if shutil.which(npm_cmd):
        return npm_cmd
    
    # 如果不在PATH中，尝试常见的安装路径（Windows）
    if sys.platform == "win32":
        common_paths = [
            os.path.expandvars(r"%ProgramFiles%\nodejs\npm.cmd"),
            os.path.expandvars(r"%ProgramFiles(x86)%\nodejs\npm.cmd"),
            os.path.expanduser(r"~\AppData\Roaming\npm\npm.cmd"),
        ]
        for path in common_paths:
            if os.path.exists(path):
                return path
    
    return None

def check_node():
    """检查Node.js是否可用"""
    node_cmd = "node.exe" if sys.platform == "win32" else "node"
    return shutil.which(node_cmd)

def main():
    """启动React前端开发服务器"""
    frontend_dir = Path(__file__).parent / "panda_web_frontend"
    
    if not frontend_dir.exists():
        print(f"前端目录不存在: {frontend_dir}")
        sys.exit(1)
    
    # 检查Node.js和npm
    print("🔍 检查Node.js和npm...")
    node_path = check_node()
    npm_path = check_npm()
    
    if not node_path:
        print("\n未找到Node.js!")
        print("请先安装Node.js (推荐版本 >= 18)")
        print("下载地址: https://nodejs.org/")
        sys.exit(1)
    
    if not npm_path:
        print("\n未找到npm!")
        print("npm通常随Node.js一起安装")
        print("请重新安装Node.js: https://nodejs.org/")
        sys.exit(1)
    
    # 获取版本信息
    try:
        node_version = subprocess.check_output([node_path, "--version"], 
                                              stderr=subprocess.STDOUT,
                                              text=True).strip()
        npm_version = subprocess.check_output([npm_path, "--version"], 
                                             stderr=subprocess.STDOUT,
                                             text=True).strip()
        print(f"Node.js: {node_version}")
        print(f"npm: {npm_version}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️  无法获取版本信息: {e}")
    
    # 检查是否已安装依赖
    node_modules = frontend_dir / "node_modules"
    if not node_modules.exists():
        print("\n📦 检测到依赖未安装，正在安装依赖...")
        print("=" * 60)
        
        # 检查是否有pnpm
        pnpm_cmd = "pnpm.cmd" if sys.platform == "win32" else "pnpm"
        try:
            subprocess.run([pnpm_cmd, "--version"], 
                          check=True, 
                          capture_output=True)
            install_cmd = [pnpm_cmd, "install"]
            print("使用 pnpm 安装依赖...")
        except (subprocess.CalledProcessError, FileNotFoundError):
            install_cmd = [npm_path, "install"]
            print("使用 npm 安装依赖...")
        
        try:
            subprocess.run(install_cmd, 
                          cwd=frontend_dir, 
                          check=True,
                          shell=sys.platform == "win32")
            print("依赖安装完成!")
        except subprocess.CalledProcessError as e:
            print(f"依赖安装失败: {e}")
            print("\n💡 尝试手动安装:")
            print(f"   cd {frontend_dir}")
            print("   npm install")
            sys.exit(1)
    
    # 检查并创建环境变量文件
    env_file = frontend_dir / ".env.development"
    env_content = """# 开发环境配置
# 根据 start_all.py 中的端口配置
VITE_API_BASE_URL=http://localhost:8111
VITE_DATAHUB_BASE_URL=http://localhost:8080
VITE_FACTOR_API_BASE_URL=http://localhost:8111
VITE_LLM_API_BASE_URL=http://localhost:8111
"""
    
    # 始终更新环境变量文件以确保端口正确
    env_file.write_text(env_content, encoding='utf-8')
    if not env_file.exists():
        print("已创建 .env.development 文件")
    else:
        print("已更新 .env.development 文件（端口: 8080/8111）")
    
    # 启动开发服务器
    print("\n🚀 启动React开发服务器...")
    print("=" * 60)
    print("📍 访问地址: http://localhost:3000")
    print("💡 提示: 确保后端服务已启动")
    print("   - 数据清洗服务: http://localhost:8080")
    print("   - 因子计算服务: http://localhost:8111")
    print("=" * 60)
    print("🔧 端口配置已更新: 8080 (数据) + 8111 (因子+LLM)")
    print()
    
    try:
        # Windows上需要使用shell=True或.cmd后缀
        subprocess.run([npm_path, "run", "dev"], 
                      cwd=frontend_dir, 
                      check=True,
                      shell=sys.platform == "win32")
    except KeyboardInterrupt:
        print("\n\n👋 开发服务器已停止")
    except subprocess.CalledProcessError as e:
        print(f"\n启动失败: {e}")
        print("\n💡 尝试手动启动:")
        print(f"   cd {frontend_dir}")
        print("   npm run dev")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n找不到命令: {e}")
        print(f"npm路径: {npm_path}")
        print("\n💡 请检查Node.js是否正确安装")
        sys.exit(1)

if __name__ == "__main__":
    main()

