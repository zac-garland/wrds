## 05_build_signals.R
## Build call-level interaction signals

library(dplyr)

qa_scored <- readRDS("research/earnings-calls/data/processed/qa_scored.rds")

call_signals <- qa_scored |>
  group_by(transcriptid, companyid, call_date) |>
  summarise(
    n_questions     = n(),
    pct_probing     = mean(tone_dict == "PROBING"),
    pct_confirming  = mean(tone_dict == "CONFIRMING"),
    pct_clarifying  = mean(tone_dict == "CLARIFYING"),
    avg_evasiveness = mean(evasiveness_score),
    probing_evasive = mean(tone_dict == "PROBING" & evasiveness_score > 0.6),
    interaction_sig = pct_probing * avg_evasiveness,
    .groups = "drop"
  )

dir.create("research/earnings-calls/data/processed", recursive = TRUE, showWarnings = FALSE)
saveRDS(call_signals, "research/earnings-calls/data/processed/call_signals.rds")

