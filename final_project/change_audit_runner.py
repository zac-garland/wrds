"""
Execute two notebook variants under the same dev-ticker settings and diff
their Top-10 console output.

Usage (from repo root):
  python final_project/change_audit_runner.py \
    --baseline "final_project/novelty_driven_mean_reversion copy.ipynb" \
    --candidate "final_project/novelty_driven_mean_reversion.ipynb"
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_TOP10_HEADER_RE = re.compile(r"^===\s*Top 10 Most Novel.*===", re.IGNORECASE)
_TICKER_LINE_RE = re.compile(r"^Ticker\s*:\s*", re.IGNORECASE)


@dataclass(frozen=True)
class RunResult:
    label: str
    top10_text: str
    full_stdout: str


def _require_nb_deps() -> tuple[object, object]:
    try:
        import nbformat  # type: ignore
        from nbclient import NotebookClient  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Missing notebook execution deps. Install with:\n"
            "  python -m pip install nbformat nbclient\n"
            f"Original import error: {e}"
        ) from e
    return nbformat, NotebookClient


def _patch_notebook_in_memory(
    nb: object,
    *,
    dev_tickers: list[str],
    ensure_ollama: bool,
) -> object:
    # nbformat notebook object: nb["cells"] list of dict-like cells.
    dev_line = f"_DEV_TICKERS_RAW = {dev_tickers!r}\n"
    ensure_line = f"ENSURE_OLLAMA = {ensure_ollama}\n"

    def _maybe_replace(cell_src: str) -> str:
        out = cell_src
        if "_DEV_TICKERS_RAW" in out:
            out = re.sub(r"^_DEV_TICKERS_RAW\s*=.*$", dev_line.rstrip("\n"), out, flags=re.M)
        if "ENSURE_OLLAMA" in out:
            out = re.sub(r"^ENSURE_OLLAMA\s*=.*$", ensure_line.rstrip("\n"), out, flags=re.M)
        return out

    # First pass: try to replace in-place if variables already exist.
    found_dev = False
    found_ensure = False
    for cell in nb["cells"]:
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        if "_DEV_TICKERS_RAW" in src:
            found_dev = True
        if "ENSURE_OLLAMA" in src:
            found_ensure = True
        new_src = _maybe_replace(src)
        if new_src != src:
            cell["source"] = new_src

    # If not found, prepend a small config cell so both notebooks behave the same.
    prepend_lines: list[str] = []
    if not found_dev:
        prepend_lines.append(dev_line)
    if not found_ensure:
        prepend_lines.append(ensure_line)
    if prepend_lines:
        nb["cells"].insert(
            0,
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {"tags": ["injected-by-change-audit-runner"]},
                "outputs": [],
                "source": "".join(prepend_lines),
            },
        )

    return nb


def _collect_stdout(nb: object) -> str:
    chunks: list[str] = []
    for cell in nb.get("cells", []):
        for out in cell.get("outputs", []) or []:
            if out.get("output_type") == "stream" and out.get("name") == "stdout":
                chunks.append("".join(out.get("text", "")))
    return "".join(chunks)


def _extract_top10(stdout: str) -> str:
    lines = stdout.splitlines()
    start_idx = None
    for i, ln in enumerate(lines):
        if _TOP10_HEADER_RE.match(ln.strip()):
            start_idx = i
            break
    if start_idx is None:
        # Fallback: just pull the first 10 "Ticker :" blocks if header isn't present.
        out: list[str] = []
        ticker_idxs = [i for i, ln in enumerate(lines) if _TICKER_LINE_RE.match(ln.strip())]
        for idx in ticker_idxs[:10]:
            out.extend(lines[idx : idx + 6])  # usually 3-5 lines per entry (+ blank)
            out.append("")
        return "\n".join(out).strip() + "\n"

    # Capture from header through the first 10 ticker blocks.
    out_lines: list[str] = [lines[start_idx]]
    ticker_seen = 0
    for ln in lines[start_idx + 1 :]:
        out_lines.append(ln)
        if _TICKER_LINE_RE.match(ln.strip()):
            ticker_seen += 1
            if ticker_seen >= 10:
                # include a small tail (novelty/salience/text lines + a blank)
                # then stop once we hit a double-blank boundary.
                pass
        if ticker_seen >= 10:
            # stop once we see two consecutive blanks (end of printed block)
            if len(out_lines) >= 3 and out_lines[-1].strip() == "" and out_lines[-2].strip() == "":
                break
    return "\n".join(out_lines).strip() + "\n"


def _run_notebook(
    path: Path,
    *,
    label: str,
    dev_tickers: list[str],
    ensure_ollama: bool,
    timeout_s: int,
) -> RunResult:
    nbformat, NotebookClient = _require_nb_deps()
    nb = nbformat.read(path, as_version=4)
    nb = _patch_notebook_in_memory(nb, dev_tickers=dev_tickers, ensure_ollama=ensure_ollama)

    client = NotebookClient(
        nb,
        timeout=timeout_s,
        kernel_name="python3",
        allow_errors=False,
        resources={"metadata": {"path": str(path.parent)}},
    )
    client.execute()

    stdout = _collect_stdout(nb)
    top10 = _extract_top10(stdout)
    return RunResult(label=label, top10_text=top10, full_stdout=stdout)


def _udiff(a: str, b: str, *, a_label: str, b_label: str) -> str:
    diff = difflib.unified_diff(
        a.splitlines(keepends=True),
        b.splitlines(keepends=True),
        fromfile=a_label,
        tofile=b_label,
    )
    return "".join(diff)


def main(argv: Iterable[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", required=True, help="Baseline notebook path (e.g. the copy).")
    p.add_argument("--candidate", required=True, help="Candidate notebook path (e.g. current).")
    p.add_argument("--timeout-s", type=int, default=60 * 60, help="Per-notebook execution timeout.")
    p.add_argument(
        "--dev-tickers",
        default="AAPL,MSFT,NVDA",
        help="Comma-separated dev tickers to force in both notebooks.",
    )
    p.add_argument(
        "--ensure-ollama",
        action="store_true",
        help="If set, run ensure_ollama inside notebook when present (default: off).",
    )
    args = p.parse_args(list(argv))

    baseline = Path(args.baseline).expanduser().resolve()
    candidate = Path(args.candidate).expanduser().resolve()
    dev_tickers = [t.strip() for t in args.dev_tickers.split(",") if t.strip()]

    if not baseline.exists():
        raise FileNotFoundError(str(baseline))
    if not candidate.exists():
        raise FileNotFoundError(str(candidate))

    print(f"[change-audit] dev_tickers={dev_tickers!r}")
    print(f"[change-audit] baseline={baseline}")
    print(f"[change-audit] candidate={candidate}")
    print(f"[change-audit] ensure_ollama={bool(args.ensure_ollama)}")
    print("")

    r0 = _run_notebook(
        baseline,
        label="baseline",
        dev_tickers=dev_tickers,
        ensure_ollama=bool(args.ensure_ollama),
        timeout_s=int(args.timeout_s),
    )
    r1 = _run_notebook(
        candidate,
        label="candidate",
        dev_tickers=dev_tickers,
        ensure_ollama=bool(args.ensure_ollama),
        timeout_s=int(args.timeout_s),
    )

    print("=== BASELINE: Top 10 ===\n")
    print(r0.top10_text)
    print("=== CANDIDATE: Top 10 ===\n")
    print(r1.top10_text)

    diff = _udiff(r0.top10_text, r1.top10_text, a_label="baseline", b_label="candidate")
    print("=== DIFF (baseline -> candidate) ===\n")
    print(diff if diff.strip() else "(no differences)")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))

