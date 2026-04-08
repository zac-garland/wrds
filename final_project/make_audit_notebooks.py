"""
Generate small iteration notebooks that call audit_top10.py.

These notebooks are intentionally tiny so you can run each iteration manually
and still get a deterministic stdout block for diffing.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _make_nb(nbformat, *, title: str, command: str):
    md = nbformat.v4.new_markdown_cell(f"## {title}\n\nRuns:\n\n`{command}`\n")
    code = nbformat.v4.new_code_cell(
        "\n".join(
            [
                "import subprocess, shlex",
                f"cmd = {command!r}",
                "print(cmd)",
                "print()",
                # Notebooks live under final_project/iterations/, but the runner lives in final_project/.
                "subprocess.run(shlex.split(cmd), check=True, cwd='..')",
            ]
        )
    )
    return nbformat.v4.new_notebook(cells=[md, code])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="iterations")
    ap.add_argument("--dev-tickers", default="AAPL,MSFT,NVDA")
    ap.add_argument("--embedding-model", default="all-MiniLM-L6-v2")
    ap.add_argument("--cache-tag", default="audit_v1")
    ap.add_argument("--use-dev-final-subset", action="store_true", default=True)
    args = ap.parse_args()

    import nbformat  # type: ignore

    here = Path(__file__).resolve().parent
    out_dir = here / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    base = "python audit_top10.py"
    common = (
        f'{base} --dev-tickers "{args.dev_tickers}" --embedding-model "{args.embedding_model}" '
        f'--cache-tag "{args.cache_tag}"'
        + (" --use-dev-final-subset" if args.use_dev_final_subset else "")
    )

    items = [
        ("iter_001_chunk.ipynb", "Iteration 001 — chunk novelty + chunk salience", common + " --mode chunk"),
        ("iter_002_turns.ipynb", "Iteration 002 — turns novelty + chunk salience", common + " --mode turns"),
        (
            "iter_003_step12_turn_boundaries.ipynb",
            "Iteration 003 — Step 12: turn boundary validation (1128909)",
            common + " --mode chunk --checkpoint step12 --check-transcriptid 1128909",
        ),
        (
            "iter_004_step11_13_15_tables.ipynb",
            "Iteration 004 — Steps 11/13/15: raw turns + exec chunks + alignment (1128909)",
            common + " --mode chunk --checkpoint step11_13_15 --check-transcriptid 1128909",
        ),
    ]

    for fn, title, cmd in items:
        nb = _make_nb(nbformat, title=title, command=cmd)
        nbformat.write(nb, out_dir / fn)

    print(f"Wrote {len(items)} notebooks to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

