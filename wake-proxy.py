"""
Reverse proxy: LiteLLM / Open WebUI -> (this) -> Ollama on the 5090 desktop.
If Ollama doesn't respond, POST to a Home Assistant webhook that sends Wake-on-LAN,
poll until Ollama is up (or time out), then forward the request.
"""
import asyncio, os, time
from aiohttp import web, ClientSession, ClientTimeout, ClientConnectorError

OLLAMA = os.environ["OLLAMA_URL"].rstrip("/")
HA_WEBHOOK = os.environ.get("HA_WEBHOOK_URL")
WAKE_TIMEOUT = int(os.environ.get("WAKE_TIMEOUT_SEC", "90"))
_last_wake = 0.0

async def ollama_up(session):
    try:
        async with session.get(f"{OLLAMA}/api/tags", timeout=ClientTimeout(total=2)) as r:
            return r.status == 200
    except Exception:
        return False

async def ensure_awake(session):
    global _last_wake
    if await ollama_up(session):
        return True
    if HA_WEBHOOK and time.time() - _last_wake > 30:       # don't spam WoL
        _last_wake = time.time()
        try:
            await session.post(HA_WEBHOOK, json={"source": "wake-proxy"}, timeout=ClientTimeout(total=5))
            print("wake-proxy: sent wake request to Home Assistant", flush=True)
        except Exception as e:
            print(f"wake-proxy: webhook failed: {e}", flush=True)
    deadline = time.time() + WAKE_TIMEOUT
    while time.time() < deadline:
        await asyncio.sleep(3)
        if await ollama_up(session):
            print("wake-proxy: desktop is up", flush=True)
            return True
    print("wake-proxy: desktop did not come up; caller will fall back", flush=True)
    return False

async def handle(request):
    session = request.app["session"]
    if not await ensure_awake(session):
        return web.json_response({"error": "local AI host unavailable"}, status=503)
    url = f"{OLLAMA}{request.rel_url}"
    body = await request.read()
    headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
    try:
        async with session.request(request.method, url, data=body, headers=headers,
                                   timeout=ClientTimeout(total=None)) as upstream:
            resp = web.StreamResponse(status=upstream.status, headers={
                k: v for k, v in upstream.headers.items()
                if k.lower() not in ("content-length", "transfer-encoding", "content-encoding")})
            await resp.prepare(request)
            async for chunk in upstream.content.iter_any():
                await resp.write(chunk)
            await resp.write_eof()
            return resp
    except ClientConnectorError:
        return web.json_response({"error": "local AI host unreachable"}, status=503)

async def on_startup(app):
    app["session"] = ClientSession()

async def on_cleanup(app):
    await app["session"].close()

app = web.Application(client_max_size=256 * 1024**2)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)
app.router.add_route("*", "/{tail:.*}", handle)
web.run_app(app, host="0.0.0.0", port=11434)
