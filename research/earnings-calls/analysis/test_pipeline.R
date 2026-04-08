## test_pipeline.R
## End-to-end smoke test for scripts 01-05
## Pulls real WRDS data (NVIDIA, 2022). Stops on any error.

library(dplyr)
library(stringr)

setwd("/Users/zacgarland/r_projects/wrds")
devtools::load_all(".", quiet = TRUE)

dir.create("research/earnings-calls/data/raw",       recursive = TRUE, showWarnings = FALSE)
dir.create("research/earnings-calls/data/processed", recursive = TRUE, showWarnings = FALSE)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 – pull transcripts
# ══════════════════════════════════════════════════════════════════════════════
cat("\n── STEP 1: transcripts ──\n")

wrds <- connect_wharton()
on.exit(DBI::dbDisconnect(wrds), add = TRUE)

## company lookup: collect the small table, then filter in R to avoid SQL translation issues
companies <- wharton_table("ciq", "ciqcompany")
if (!is.null(companies$error)) stop("ciq.ciqcompany: ", companies$error)

company_lookup <- companies$result |>
  select(companyid, companyname) |>
  collect() |>
  filter(companyname == "NVIDIA Corporation")

cat("Company matches:\n"); print(company_lookup)
if (nrow(company_lookup) == 0) stop("No matching companies found.")

company_ids <- company_lookup$companyid
id_string   <- paste0(company_ids, collapse = ",")

query <- glue::glue("
  SELECT
    a.transcriptid,
    a.companyid,
    a.mostimportantdateutc        AS call_date,
    a.transcriptcollectiontypeid,
    a.transcriptpresentationtypename,
    b.transcriptcomponentid,
    b.componentorder,
    b.speakertypeid,
    b.transcriptpersonid,
    b.transcriptpersonname        AS speakername,
    c.componenttext
  FROM (
    SELECT *
    FROM ciq.wrds_transcript_detail
    WHERE companyid IN ({id_string})
      AND keydeveventtypeid = 48
      AND date_part('year', mostimportantdateutc) = 2022
  ) AS a
  JOIN ciq.wrds_transcript_person AS b
    ON a.transcriptid = b.transcriptid
  JOIN ciq.ciqtranscriptcomponent AS c
    ON b.transcriptcomponentid = c.transcriptcomponentid
  ORDER BY a.transcriptid, b.componentorder
")

res <- DBI::dbSendQuery(wrds, query)
raw <- DBI::dbFetch(res, n = -1)
DBI::dbClearResult(res)

cat(sprintf("Pulled %d rows across %d transcripts (pre-dedup)\n",
            nrow(raw), n_distinct(raw$transcriptid)))
if (nrow(raw) == 0) stop("Query returned no rows.")

## Deduplicate: per (companyid, call_date) keep best version
## Priority: Final > Preliminary, then lowest transcriptcollectiontypeid, then lowest transcriptid
best_ids <- raw |>
  distinct(transcriptid, companyid, call_date,
           transcriptcollectiontypeid, transcriptpresentationtypename) |>
  mutate(is_final = transcriptpresentationtypename == "Final") |>
  group_by(companyid, call_date) |>
  arrange(desc(is_final), transcriptcollectiontypeid, transcriptid) |>
  slice(1) |>
  ungroup() |>
  pull(transcriptid)

raw <- raw |> filter(transcriptid %in% best_ids)

cat(sprintf("After dedup: %d rows across %d transcripts\n",
            nrow(raw), n_distinct(raw$transcriptid)))

saveRDS(raw, "research/earnings-calls/data/raw/transcripts_raw.rds")
cat("Saved transcripts_raw.rds\n")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 – parse Q&A
# ══════════════════════════════════════════════════════════════════════════════
cat("\n── STEP 2: parse Q&A ──\n")

calls <- raw |>
  group_by(transcriptid) |>
  mutate(
    is_qa_start = speakertypeid == 1 &
      str_detect(
        tolower(componenttext),
        "question.{0,30}answer|q&a|open.*floor|take.*question|first question"
      ),
    qa_section = cumsum(is_qa_start) > 0
  ) |>
  ungroup()

qa_only <- calls |> filter(qa_section)

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
    analyst_name  = first(speakername[speakertypeid == 3L]),
    question_text = paste(componenttext[speakertypeid == 3], collapse = " "),
    response_text = paste(componenttext[speakertypeid == 2], collapse = " "),
    .groups       = "drop"
  ) |>
  filter(
    nchar(question_text) > 20,
    nchar(response_text) > 20
  )

cat(sprintf("Q&A pairs: %d across %d transcripts\n",
            nrow(qa_pairs), n_distinct(qa_pairs$transcriptid)))
if (nrow(qa_pairs) == 0) stop("No Q&A pairs found — check speakertypeid values or Q&A detection regex.")

saveRDS(qa_pairs, "research/earnings-calls/data/processed/qa_pairs.rds")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 – classify tone
# ══════════════════════════════════════════════════════════════════════════════
cat("\n── STEP 3: classify tone ──\n")

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
  c_ <- sum(str_detect(tl, confirming_terms))
  dplyr::case_when(
    p > c_ ~ "PROBING",
    c_ > p ~ "CONFIRMING",
    TRUE   ~ "CLARIFYING"
  )
}

qa_classified <- qa_pairs |>
  mutate(tone_dict = vapply(question_text, score_tone, character(1)))

print(table(qa_classified$tone_dict))
saveRDS(qa_classified, "research/earnings-calls/data/processed/qa_classified.rds")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 – score evasiveness
# ══════════════════════════════════════════════════════════════════════════════
cat("\n── STEP 4: evasiveness ──\n")

library(text2vec)

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

cat("Evasiveness score summary:\n")
print(summary(qa_scored$evasiveness_score))
saveRDS(qa_scored, "research/earnings-calls/data/processed/qa_scored.rds")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 – build call-level signals
# ══════════════════════════════════════════════════════════════════════════════
cat("\n── STEP 5: call signals ──\n")

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

print(call_signals)
saveRDS(call_signals, "research/earnings-calls/data/processed/call_signals.rds")

cat("\n✓ Pipeline steps 01–05 complete.\n")
