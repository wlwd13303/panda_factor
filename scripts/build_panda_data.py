"""
打包 panda_common + panda_data 为 wheel 文件，方便分发
用法:
    python scripts/build_panda_data.py          # 构建两个 wheel 到 dist/ 目录
    python scripts/build_panda_data.py --install  # 构建并安装
"""

import subprocess
import sys
import os
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(ROOT, "dist")

os.makedirs(DIST, exist_ok=True)

# 先构建 panda_common，再构建 panda_data（panda_data 依赖 panda_common）
for name in ("panda_common", "panda_data"):
    path = os.path.join(ROOT, name)
    print(f"\n{'='*50}")
    print(f"构建 {name}...")
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", DIST, path],
        check=True,
        cwd=ROOT,
    )

# 清理旧 wheel（只保留最新的）
wheels = {}
for f in os.listdir(DIST):
    if f.endswith(".whl"):
        pkg = f.split("-")[0]
        if pkg not in wheels:
            wheels[pkg] = []
        wheels[pkg].append(os.path.join(DIST, f))

for pkg, files in wheels.items():
    if len(files) > 1:
        files.sort(key=os.path.getmtime)
        for old in files[:-1]:
            os.remove(old)
            print(f"  清理旧版本: {os.path.basename(old)}")

print(f"\n构建完成，输出目录: {DIST}")
for f in sorted(os.listdir(DIST)):
    if f.endswith(".whl"):
        print(f"  {f}")

print(f"\n安装命令:")
print(f"  uv pip install {DIST}\\panda_common-*.whl {DIST}\\panda_data-*.whl")

if "--install" in sys.argv:
    print("\n安装到当前环境...")
    subprocess.run(
        ["uv", "pip", "install", "--reinstall"] + [os.path.join(DIST, f) for f in os.listdir(DIST) if f.endswith(".whl")],
        check=True,
        cwd=ROOT,
    )
    print("安装完成")
