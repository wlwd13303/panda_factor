import os
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from panda_common.config import config
from fastapi.responses import RedirectResponse

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

# Redirect root to /factor
@app.get("/")
async def redirect_to_factor():
    return RedirectResponse(url="/factor")

def main():
    """Entry point for the web service"""
    port = config.get("web_service", {}).get("port", 8080)
    host = config.get("web_service", {}).get("host", "0.0.0.0")
    
    print(f"Starting web service on {host}:{port}")
    print(f"Serving Vue dist files from: {DIST_DIR}")
    
    uvicorn.run(
        "panda_web.main:app",
        host=host,
        port=port,
        reload=True  # Enable auto-reload during development
    )

if __name__ == "__main__":
    main() 