"""Build-only provisioning of a pinned, offline spoken-name checker."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.request
import venv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from video_followup_speech_guard import MODEL_HASHES, MODEL_ID, MODEL_MANIFEST, MODEL_REVISION, verify_model_assets


def provision_model(directory):
    directory.mkdir(parents=True, exist_ok=True)
    for filename, digest in MODEL_HASHES.items():
        target = directory / filename
        if target.is_file() and not target.is_symlink():
            with target.open("rb") as existing:
                if hashlib.file_digest(existing, "sha256").hexdigest() == digest:
                    continue
        url = f"https://huggingface.co/{MODEL_ID}/resolve/{MODEL_REVISION}/{filename}"
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
                temporary = Path(output.name)
                checksum, total = hashlib.sha256(), 0
                with urllib.request.urlopen(url, timeout=60) as response:
                    if not response.url.startswith("https://"):
                        raise RuntimeError("Model download requires HTTPS")
                    while chunk := response.read(1024 * 1024):
                        total += len(chunk)
                        if total > 100 * 1024 * 1024:
                            raise RuntimeError("Model asset exceeds the bounded download size")
                        output.write(chunk)
                        checksum.update(chunk)
                if checksum.hexdigest() != digest:
                    raise RuntimeError("Pinned speech model asset checksum mismatch")
            os.replace(temporary, target)
            temporary = None
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)
    (directory / "manifest.json").write_text(json.dumps(MODEL_MANIFEST, sort_keys=True) + "\n")
    verify_model_assets(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / ".video-speech-guard")
    parser.add_argument("--model-only", action="store_true", help="Provision assets only; does not certify runtime readiness")
    args = parser.parse_args()
    root = args.root.resolve()
    provision_model(root / "model")
    if not args.model_only:
        venv.EnvBuilder(with_pip=True).create(root / "venv")
        python = root / "venv/bin/python"
        subprocess.run([str(python), "-m", "pip", "install", "--only-binary=:all:", "-r",
                        str(ROOT / "requirements-video-speech-guard.txt")], check=True, timeout=600)
        environment = {**os.environ, "VIDEO_FOLLOWUP_SPEECH_PYTHON": str(python),
                       "VIDEO_FOLLOWUP_SPEECH_MODEL_DIR": str(root / "model")}
        subprocess.run([sys.executable, "-c", "from video_followup_speech_guard import self_test; self_test()"],
                       cwd=ROOT, env=environment, check=True, timeout=60)
    print("Pinned video speech-guard assets provisioned; no generation or messaging performed.")


if __name__ == "__main__":
    main()
