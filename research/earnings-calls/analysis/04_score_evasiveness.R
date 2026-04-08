## 04_score_evasiveness.R
## Baseline evasiveness scoring via cosine similarity (text2vec)

library(dplyr)
library(text2vec)

qa_classified <- readRDS("research/earnings-calls/data/processed/qa_classified.rds")

compute_cosine_similarity <- function(q_text, r_text) {
  corpus <- c(q_text, r_text)
  it     <- itoken(corpus, tokenizer = word_tokenizer)
  vocab  <- create_vocabulary(it)
  vect   <- vocab_vectorizer(vocab)
  dtm    <- create_dtm(itoken(corpus, tokenizer = word_tokenizer), vect)
  sim    <- sim2(dtm[1, , drop = FALSE], dtm[2, , drop = FALSE], method = "cosine")
  as.numeric(sim)
}

qa_scored <- qa_classified |>
  mutate(
    evasiveness_raw   = mapply(compute_cosine_similarity, question_text, response_text),
    evasiveness_score = 1 - evasiveness_raw
  )

dir.create("research/earnings-calls/data/processed", recursive = TRUE, showWarnings = FALSE)
saveRDS(qa_scored, "research/earnings-calls/data/processed/qa_scored.rds")

