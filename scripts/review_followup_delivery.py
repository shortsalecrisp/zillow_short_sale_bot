"""Read-only verifier survey and atomic local carry-forward ledger; never sends."""
import argparse
import fcntl
import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.request import Request, build_opener, ProxyHandler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from followup_delivery_review import merge_ledger

URL = "https://zillow-short-sale-bot.onrender.com/internal/review-followup-sms"


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".followup-review-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(data, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--retry-after", required=True, help="Next existing verifier slot, ISO timestamp")
    args=parser.parse_args()
    if args.output.exists() or args.output.resolve()==args.ledger.resolve():
        raise ValueError("Output must be new and separate from the ledger")
    token=(Path.home()/".codex/automations/lead-verifier-sms-token").read_text().strip()
    request=Request(URL,headers={"Authorization":"Bearer "+token})
    with build_opener(ProxyHandler({})).open(request,timeout=180) as response:
        raw=response.read(32_000_001)
    if len(raw)>32_000_000:
        raise ValueError("Survey exceeds bounded response size")
    survey=json.loads(raw)
    save(args.output,survey)
    args.ledger.parent.mkdir(parents=True,exist_ok=True)
    with open(args.ledger.with_suffix(".lock"),"a",opener=lambda p,f:os.open(p,f,0o600)) as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        previous=json.loads(args.ledger.read_text()) if args.ledger.exists() else None
        merged=merge_ledger(previous,survey,args.retry_after)
        save(args.ledger,merged)
    print(json.dumps({"complete":survey["complete"],"counts":survey["counts"],
                      "due_unsent_rows":[x["row"] for x in survey["items"] if x["status"]=="due_unsent"],
                      "corrected_phone_review_rows":[x["row"] for x in survey.get("remediation_queue",[])],
                      "carried_corrected_phone_review_rows":[x["row"] for x in merged.get("remediation_queue",[])],
                      "outreach_calls":0}))


if __name__=="__main__":
    main()
