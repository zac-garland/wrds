# Access full-text CIQ transcripts for Mastercard, Visa, American Express
# Uses wrds package: connect_wharton(), wharton_table(), dbplyr

library(dplyr)
library(dbplyr)

# Load wrds package (run from project root: devtools::load_all(".") or library(wrds))
con <- connect_wharton()

# ---- 1. Look up CIQ company IDs for Mastercard, Visa, American Express ----

companies <- wharton_table("ciq", "ciqcompany")
if (!is.null(companies$error)) stop("ciq.ciqcompany: ", companies$error)

# Column is usually 'companyname'; if query fails, check WRDS table
company_lookup <- companies$result %>%
  filter(
    dbplyr::sql("LOWER(companyname) LIKE '%mastercard%' OR
                LOWER(companyname) LIKE '%visa%' OR
                LOWER(companyname) LIKE '%american express%'")
  ) %>%
  select(companyid, companyname) %>%
  collect()

print(company_lookup)
companyIdValues <- company_lookup$companyid

begYear <- 2015L
endYear <- 2017L

# ---- 2. Transcript tables (dbplyr) ----

detail <- wharton_table("ciq", "wrds_transcript_detail")$result %>% filter(transcriptid == 3639441) %>% collect()
person <- wharton_table("ciq", "wrds_transcript_person")$result %>% filter(transcriptid == 3639441) %>% collect()
component <- wharton_table("ciq", "ciqtranscriptcomponent")$result %>% filter(transcriptid == 3639441) %>% collect()

if (!is.null(detail$error))   stop("ciq.wrds_transcript_detail: ", detail$error)
if (!is.null(person$error))   stop("ciq.wrds_transcript_person: ", person$error)
if (!is.null(component$error)) stop("ciq.ciqtranscriptcomponent: ", component$error)

a <- detail$result %>%
  filter(
    companyid %in% !!companyIdValues,
    dbplyr::sql(paste0("date_part('year', mostimportantdateutc) BETWEEN ", begYear, " AND ", endYear))
  )

data <- a %>%
  inner_join(person$result, by = "transcriptid") %>%
  inner_join(component$result, by = "transcriptcomponentid") %>%
  arrange(transcriptid, componentorder) %>%
  collect()

# ---- 3. Inspect ----

head(data)
# View(data)

# Optional: save
# write.csv(data, "ciq_transcripts_mastercard_visa_amex_2015_2017.csv", row.names = FALSE)
