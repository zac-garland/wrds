# Earnings Call Interaction Framework — Case Study (Earnings Calls Subproject)

This subproject lives under `research/earnings-calls/` inside your `wrds` repo and reuses:
- `R/` package functions (via `devtools::load_all(".")`)
- `notes/` for WRDS table navigation, CRSP/Compustat linkage, etc.

The core idea is unchanged:
- **Analyst Question Tone Classification** (probing / confirming / clarifying)
- **Management Evasiveness Scoring** (semantic similarity question ↔ response)

The 2D interaction (tone × evasiveness) is then linked to CRSP returns.

For full methodological detail, see the original `CLAUDE.md` in `Downloads` and the scripts in `research/earnings-calls/analysis/`.

