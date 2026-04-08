## 02_parse_qa.R
## Parse Q&A section and build question–response pairs

library(dplyr)
library(stringr)

raw <- readRDS("research/earnings-calls/data/raw/transcripts_raw.rds")

# 2a. Split prepared remarks vs Q&A ---------------------------------------

calls <- raw |>
  group_by(transcriptid) |>
  mutate(
    is_qa_start = speakertypeid == 1 & # operator turn
      str_detect(
        tolower(componenttext),
        "question.{0,30}answer|q&a|open.*floor|take.*question|first question"
      ),
    qa_section = cumsum(is_qa_start) > 0
  ) |>
  ungroup()

qa_only <- calls |> filter(qa_section)

# 2b. Build question–response pairs ---------------------------------------

qa_pairs <- qa_only |>
  group_by(transcriptid) |>
  mutate(
    is_analyst_q = speakertypeid == 3,
    question_id  = cumsum(is_analyst_q)
  ) |>
  filter(question_id > 0) |>
  group_by(transcriptid, question_id) |>
  summarise(
    call_date     = first(call_date),
    companyid     = first(companyid),
    analyst_name  = first(speakername[speakertypeid == 3]),
    question_text = paste(componenttext[speakertypeid == 3], collapse = " "),
    response_text = paste(componenttext[speakertypeid == 2], collapse = " "),
    .groups       = "drop"
  ) |>
  filter(
    nchar(question_text) > 20,
    nchar(response_text) > 20
  )

dir.create("research/earnings-calls/data/processed", recursive = TRUE, showWarnings = FALSE)
saveRDS(qa_pairs, "research/earnings-calls/data/processed/qa_pairs.rds")

