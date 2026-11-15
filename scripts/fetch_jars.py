"""
Download the JVM jars listed in jars/jars.lock into jars/ and verify each SHA-1.

Standard library only, so it runs before (or without) `pip install`.
Already-present jars with the right checksum are skipped, so re-running is cheap.

Usage:
    python scripts/fetch_jars.py           # download missing jars
    python scripts/fetch_jars.py --check   # verify only, download nothing
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
JARS_DIR = REPO_ROOT / "jars"
LOCK_FILE = JARS_DIR / "jars.lock"
MAVEN = "https://repo1.maven.org/maven2"


def read_lock(lock: Path = LOCK_FILE) -> list[tuple[str, str, str, str]]:
    """[(group, artifact, version, sha1)] from jars.lock."""
    entries = []
    for line in lock.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        coord, sha1 = line.split()
        group, artifact, version = coord.split(":")
        entries.append((group, artifact, version, sha1.lower()))
    return entries


def jar_name(artifact: str, version: str) -> str:
    return f"{artifact}-{version}.jar"


def sha1_of(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(group: str, artifact: str, version: str, sha1: str) -> None:
    target = JARS_DIR / jar_name(artifact, version)
    url = f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{jar_name(artifact, version)}"
    tmp = target.with_suffix(".jar.part")
    print(f"  downloading {url}", flush=True)
    with urllib.request.urlopen(url) as resp, tmp.open("wb") as out:  # noqa: S310 - fixed https host
        shutil.copyfileobj(resp, out, length=1 << 20)  # streamed: the AWS bundle is ~700 MB
    actual = sha1_of(tmp)
    if actual != sha1:
        tmp.unlink()
        raise SystemExit(f"SHA-1 mismatch for {target.name}: expected {sha1}, got {actual}")
    tmp.replace(target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="Verify only, download nothing")
    args = parser.parse_args()

    JARS_DIR.mkdir(exist_ok=True)
    problems = 0
    for group, artifact, version, sha1 in read_lock():
        path = JARS_DIR / jar_name(artifact, version)
        if path.exists() and sha1_of(path) == sha1:
            print(f"ok       {path.name}")
            continue
        if args.check:
            print(f"MISSING  {path.name}" if not path.exists() else f"BAD SHA1 {path.name}")
            problems += 1
            continue
        fetch(group, artifact, version, sha1)
        print(f"fetched  {path.name}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
