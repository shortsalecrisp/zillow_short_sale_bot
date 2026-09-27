"""Mobile takeover gateway. Credentials stay in the URL fragment and POST body."""

import re
import secrets
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool


PAGE = """<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stop automated replies</title><style nonce="__NONCE__">
body{font:18px Georgia,serif;background:#f3f5f2;color:#182b25;margin:0;padding:32px 24px}
main{max-width:520px;margin:12vh auto}h1{font-size:30px}button{font:inherit;padding:12px 24px;cursor:pointer}
</style></head><body><main><h1 id="title">Stopping automated replies...</h1>
<p id="message">Please keep this page open while we confirm manual takeover.</p>
<button id="retry" hidden>Retry stopping replies</button></main>
<script nonce="__NONCE__">
const params=new URLSearchParams(location.hash.slice(1));
const token=params.get('token')||'',phone=params.get('phone')||'';
history.replaceState(null,'',location.pathname);
const title=document.getElementById('title'),message=document.getElementById('message'),retry=document.getElementById('retry');
let running=false,finished=false;
async function stop(){
  if(running||finished||document.visibilityState!=='visible'||document.prerendering)return;
  if(!token||!phone){title.textContent='Takeover link incomplete';message.textContent='Open the complete takeover link from the forwarded SMS.';return;}
  running=true;retry.hidden=true;
  title.textContent='Stopping automated replies...';
  for(let attempt=0;attempt<3;attempt++){
    try{
      const response=await fetch('/sms-takeover',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({token,phone}),signal:AbortSignal.timeout(65000),cache:'no-store',credentials:'omit'});
      const result=await response.json();
      if(response.ok&&result.ok===true){
        finished=true;title.textContent='Automated replies stopped';
        message.textContent='Manual takeover is active for '+result.phone+'. You can now reply yourself. Messages already handed to the phone cannot be recalled.';
        running=false;return;
      }
      if(result.retryable===false)break;
    }catch(_){ }
    message.textContent='Still confirming the stop. Retrying automatically...';
    if(attempt<2)await new Promise(resolve=>setTimeout(resolve,2000*(attempt+1)));
  }
  running=false;title.textContent='Stop not confirmed';
  message.textContent='We could not confirm manual takeover. Do not assume automated replies are stopped. Tap Retry, or ask for help with the bot.';
  retry.hidden=false;
}
retry.addEventListener('click',stop);
document.addEventListener('visibilitychange',stop);
document.addEventListener('prerenderingchange',stop);
stop();
</script></body></html>"""


def _phone(value):
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else ""


def relay_takeover(upstream_url, token, phone, post=requests.post):
    # The upstream is server configuration, never a caller-controlled relay URL.
    parsed = urlparse(upstream_url)
    if (parsed.scheme != "https" or parsed.netloc != "script.google.com"
            or not re.fullmatch(r"/macros/s/[A-Za-z0-9_-]+/exec", parsed.path)
            or parsed.query or parsed.fragment):
        return 503, {"ok": False, "retryable": False}
    try:
        response = post(upstream_url, json={
            "action": "takeover", "phone": phone, "value": "TRUE", "token": token,
        }, timeout=(5, 50), allow_redirects=True)
        if response.status_code != 200:
            return 502, {"ok": False, "retryable": True}
        result = response.json()
    except (requests.RequestException, ValueError):
        return 502, {"ok": False, "retryable": True}
    if not isinstance(result, dict):
        return 502, {"ok": False, "retryable": True}
    if str(result.get("error", "")).lower().endswith("unauthorized"):
        return 403, {"ok": False, "retryable": False}
    cancelled = result.get("pending_send")
    if (result.get("ok") is not True or result.get("human_override") != "TRUE"
            or _phone(result.get("phone")) != phone
            or not isinstance(cancelled, dict) or cancelled.get("ok") is not True):
        return 502, {"ok": False, "retryable": True}
    return 200, {"ok": True, "phone": phone, "queued": result.get("queued") is True}


def create_takeover_router(upstream_url, post=requests.post):
    router = APIRouter()
    headers = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
               "X-Content-Type-Options": "nosniff", "X-Robots-Tag": "noindex, nofollow"}

    @router.get("/sms-takeover")
    async def page():
        # GET/link previews never mutate state. Only the visible page submits POST.
        nonce = secrets.token_urlsafe(24)
        policy = (f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
                  "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
        return HTMLResponse(PAGE.replace("__NONCE__", nonce), headers={**headers, "Content-Security-Policy": policy})

    @router.post("/sms-takeover")
    async def takeover(request: Request):
        origin = request.headers.get("origin")
        if origin and urlparse(origin).netloc != request.headers.get("host"):
            return JSONResponse({"ok": False, "retryable": False}, status_code=403, headers=headers)
        raw = await request.body()
        if len(raw) > 2048:
            return JSONResponse({"ok": False, "retryable": False}, status_code=400, headers=headers)
        try:
            body = await request.json()
        except ValueError:
            body = None
        token = body.get("token") if isinstance(body, dict) else None
        phone = _phone(body.get("phone")) if isinstance(body, dict) else ""
        if not isinstance(token, str) or not 1 <= len(token) <= 512 or not phone:
            return JSONResponse({"ok": False, "retryable": False}, status_code=400, headers=headers)
        status, result = await run_in_threadpool(relay_takeover, upstream_url, token, phone, post)
        return JSONResponse(result, status_code=status, headers=headers)

    return router
