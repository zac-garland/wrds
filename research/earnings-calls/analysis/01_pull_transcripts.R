## 01_pull_transcripts.R
## Pull CIQ earnings call transcripts for case study firms
## Uses wrds package helpers: connect_wharton(), wharton_table()

library(DBI)
library(dplyr)
library(stringr)

devtools::load_all(".")
wrds <- connect_wharton()

## 1. Look up CIQ company IDs ---------------------------------------------

# Exact company names as they appear in ciq.ciqcompany
# Use collect() first then filter in R to avoid SQL translation issues with grepl
firm_exact <- c(
  "Mastercard Incorporated",
  "JPMorgan Chase & Co.",
  "NVIDIA Corporation",
  "Microsoft Corporation",
  "Johnson & Johnson",
  "UnitedHealth Group Incorporated",
  "Exxon Mobil Corporation",
  "Caterpillar Inc.",
  "Amazon.com, Inc.",
  "AT&T Inc."
)

companies <- wharton_table("ciq", "ciqcompany")
if (!is.null(companies$error)) stop("ciq.ciqcompany: ", companies$error)

company_lookup <- companies$result %>%
  select(companyid, companyname) %>%
  collect() %>%
  filter(companyname %in% firm_exact)

print(company_lookup)

# After inspecting, you can refine / override company_ids if needed
company_ids <- company_lookup$companyid
id_string   <- paste0(company_ids, collapse = ",")

## 2. Pull earnings call transcripts --------------------------------------

beg_year <- 2018
end_year <- 2023

query <- glue::glue("
  SELECT
    a.transcriptid,
    a.companyid,
    a.mostimportantdateutc              AS call_date,
    a.transcriptcollectiontypeid,
    a.transcriptpresentationtypename,
    b.transcriptcomponentid,
    b.componentorder,
    b.speakertypeid,                    -- 1 = Operator, 2 = Company Rep, 3 = Analyst
    b.transcriptpersonid,
    b.transcriptpersonname              AS speakername,
    c.componenttext
  FROM (
    SELECT *
    FROM ciq.wrds_transcript_detail
    WHERE companyid IN ({id_string})
      AND keydeveventtypeid = 48        -- Earnings Calls only
      AND date_part('year', mostimportantdateutc) BETWEEN {beg_year} AND {end_year}
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

## 3. Deduplicate: one transcript per (companyid, call_date) --------------
## Priority: Final > Preliminary, then Proofed > Edited > Spellchecked
## (transcriptcollectiontypeid: 1=Proofed, 2=Edited, 7=Spellchecked)

best_ids <- raw %>%
  distinct(transcriptid, companyid, call_date,
           transcriptcollectiontypeid, transcriptpresentationtypename) %>%
  mutate(is_final = transcriptpresentationtypename == "Final") %>%
  group_by(companyid, call_date) %>%
  arrange(desc(is_final), transcriptcollectiontypeid, transcriptid) %>%
  slice(1) %>%
  ungroup() %>%
  pull(transcriptid)

raw <- raw %>% filter(transcriptid %in% best_ids)

cat(sprintf("After dedup: %d rows across %d transcripts\n",
            nrow(raw), n_distinct(raw$transcriptid)))

dir.create("research/earnings-calls/data/raw", recursive = TRUE, showWarnings = FALSE)
saveRDS(raw, "research/earnings-calls/data/raw/transcripts_raw.rds")
