"""Source-state fingerprints used to keep semantic cache entries fresh."""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess


def _run_git(cwd: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=30,
        check=False,
    )


def source_fingerprint(cwd: Path) -> str | None:
    """Fingerprint Git HEAD, tracked changes, and untracked file contents.

    Cache reuse is deliberately disabled outside Git repositories because a
    trustworthy cheap source identity is unavailable there.
    """
    root_probe = _run_git(cwd, "rev-parse", "--show-toplevel")
    if root_probe.returncode != 0 or not root_probe.stdout.strip():
        return None
    root = Path(root_probe.stdout.decode("utf-8", errors="replace").strip()).resolve()

    head_probe = _run_git(root, "rev-parse", "HEAD")
    head = head_probe.stdout.strip() if head_probe.returncode == 0 else b"UNBORN"
    diff_probe = _run_git(root, "diff", "--binary", "HEAD", "--")
    if diff_probe.returncode not in {0, 1}:
        return None
    untracked_probe = _run_git(root, "ls-files", "--others", "--exclude-standard", "-z")
    if untracked_probe.returncode != 0:
        return None

    digest = hashlib.sha256()
    digest.update(b"head\0" + head + b"\0diff\0" + diff_probe.stdout)
    for raw_name in sorted(name for name in untracked_probe.stdout.split(b"\0") if name):
        candidate = root / raw_name.decode("utf-8", errors="surrogateescape")
        digest.update(b"\0untracked\0" + raw_name + b"\0")
        try:
            with candidate.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        except OSError:
            return None
    return digest.hexdigest()
