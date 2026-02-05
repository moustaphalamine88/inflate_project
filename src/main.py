from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time

from src.api.routes import router
from src.db.indexes import create_indexes
from src.db.mongo import init_mongo, close_mongo, get_db
from src.services.http_client import init_http_client, close_http_client, get_http_client

app = FastAPI(title="Support Ticket Analysis System")


@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    """
    Enforce a soft performance budget for /stats.
    If the aggregation exceeds 2 seconds, return 504.
    """
    if request.url.path.endswith("/stats"):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        if process_time > 2.0:
            return JSONResponse(
                status_code=504,
                content={"detail": "Performance Limit Exceeded: Aggregation took too long (> 2s)"},
            )
        return response

    return await call_next(request)


@app.on_event("startup")
async def startup_event():
    """
    Initialize shared resources once per process:
    - MongoDB client/pool
    - HTTPX AsyncClient (connection pool)
    - MongoDB indexes
    """
    await init_mongo()
    await init_http_client()
    await create_indexes()


@app.on_event("shutdown")
async def shutdown_event():
    """
    Clean shutdown: close pooled resources.
    """
    await close_http_client()
    await close_mongo()


app.include_router(router)


@app.get("/health")
async def health_check():
    """
    Task 5: System Health Monitoring

    Return non-200 if a critical dependency is unavailable.
    Dependencies:
    - MongoDB
    - Mock external API (optional but recommended)
    """
    # 1) Check MongoDB
    try:
        db = await get_db()
        # quick ping
        await db.command("ping")
        mongo_ok = True
    except Exception:
        mongo_ok = False

    # 2) Check external mock API (lightweight call)
    external_ok = True
    try:
        client = get_http_client()
        resp = await client.get("http://mock-external-api:9000/health", timeout=2.0)
        external_ok = resp.status_code == 200
    except Exception:
        external_ok = False

    status = {
        "status": "ok" if (mongo_ok and external_ok) else "degraded",
        "dependencies": {
            "mongo": "ok" if mongo_ok else "down",
            "external_api": "ok" if external_ok else "down",
        },
    }

    # Non-200 if any dependency is down (per spec)
    if not (mongo_ok and external_ok):
        return JSONResponse(status_code=503, content=status)

    return status
