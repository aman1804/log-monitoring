from fastapi import FastAPI
from api.routes import router

app = FastAPI(
    title="Real-Time Log Monitoring API",
    version="1.0"
)

app.include_router(router)