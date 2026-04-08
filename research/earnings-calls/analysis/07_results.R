## 07_results.R
## Basic summaries and plots for interaction signals

library(dplyr)

call_signals <- readRDS("research/earnings-calls/data/processed/call_signals.rds")

# Example: simple summary by companyid
summary_by_firm <- call_signals |>
  group_by(companyid) |>
  summarise(
    n_calls        = n(),
    avg_pct_probing = mean(pct_probing),
    avg_evasive     = mean(avg_evasiveness),
    avg_interaction = mean(interaction_sig),
    .groups = "drop"
  )

print(summary_by_firm)

# Plots / tables for export can be added here as you iterate with Claude Code.

