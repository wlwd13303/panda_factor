from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from panda_server.routes import user_factor_pro
from panda_llm.routes import chat_router
app = FastAPI(
    title="Panda Server",
    description="Server for Panda AI Factor System",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
# app.include_router(user_factor.router, prefix="/api/v1", tags=["user_factors"])
app.include_router(user_factor_pro.router, prefix="/api/v1", tags=["user_factors"])
app.include_router(chat_router.router, prefix="/llm", tags=["panda_llm"])

@app.get("/")
async def home():
    return {"message": "Welcome to the Panda Server!"}

def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8111)

if __name__ == "__main__":
    main()