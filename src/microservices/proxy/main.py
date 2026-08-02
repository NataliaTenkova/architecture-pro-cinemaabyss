import os
import random

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
import httpx


PORT = int(os.getenv("PORT", 8000))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://localhost:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))


client = httpx.AsyncClient(timeout=30.0)
app = FastAPI()

def should_use_new_service() -> bool:
    if not GRADUAL_MIGRATION:
        return False

    return random.randrange(1, 100) < MOVIES_MIGRATION_PERCENT


async def proxy_request(request: Request, target_url: str) -> Response:
    full_url = f"{target_url}{request.url.path}"

    if request.query_params:
        full_url += f"?{request.query_params}"

    try:
        response = await client.request(
            method=request.method,
            url=full_url,
            headers=dict(request.headers),
            content=await request.body()
        )

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )
    except httpx.TimeoutException:
        return JSONResponse(
            status_code=504,
            content={"error": "Gateway Timeout", "message": "Upstream service timed out"}
        )
    except httpx.ConnectError:
        return JSONResponse(
            status_code=503,
            content={"error": "Service Unavailable", "message": "Upstream service unreachable"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "message": str(e)}
        )


@app.api_route("/api/movies/{path:path}", methods=["GET", "POST"])
@app.api_route("/api/movies", methods=["GET", "POST"])
async def movies_proxy(request: Request):
    if should_use_new_service():
        return await proxy_request(request, MOVIES_SERVICE_URL)
    else:
        return await proxy_request(request, MONOLITH_URL)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def default_proxy(request: Request):
    """Все остальные запросы на монолит"""
    return await proxy_request(request, MONOLITH_URL)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)