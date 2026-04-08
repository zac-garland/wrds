## 03_classify_questions.R
## Stubs for analyst question tone classification

library(dplyr)
library(stringr)

qa_pairs <- readRDS("research/earnings-calls/data/processed/qa_pairs.rds")

## Option B: dictionary-based baseline (pure R, no API) -------------------

probing_terms <- c(
  "concern", "worried", "miss", "disappoint", "pressure", "headwind",
  "decline", "weak", "risk", "challenge", "why did", "why has",
  "how do you explain", "what went wrong", "shortfall"
)

confirming_terms <- c(
  "excited", "strong momentum", "great quarter", "impressive",
  "how should we think", "help us size the opportunity",
  "confident", "reaffirm"
)

score_tone <- function(text) {
  tl <- tolower(text)
  p  <- sum(str_detect(tl, probing_terms))
  c  <- sum(str_detect(tl, confirming_terms))
  dplyr::case_when(
    p > c  ~ "PROBING",
    c > p  ~ "CONFIRMING",
    TRUE   ~ "CLARIFYING"
  )
}

qa_classified <- qa_pairs |>
  mutate(tone_dict = vapply(question_text, score_tone, character(1)))

dir.create("research/earnings-calls/data/processed", recursive = TRUE, showWarnings = FALSE)
saveRDS(qa_classified, "research/earnings-calls/data/processed/qa_classified.rds")

# Option A (LLM / Anthropic API) can be added here later; see top-level CLAUDE.md

