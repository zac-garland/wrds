
library(tidyverse)
input_files <- list.files(path.expand("~/Downloads/Raw Transcripts"),full.names = TRUE)

input_files %>% walk(~{
  print(.x)
  vroom::vroom(.x) %>%
    group_by(companyid,mostimportantdateutc) %>%
    filter(transcriptcreationdate_utc == min(transcriptcreationdate_utc,na.rm=TRUE)) %>%
    arrange(companyid,transcriptid) %>%
    slice(1) %>% # still seems to be duplicates even if we take first transcript creation date (e.g. date was uploaded same but multiple on the same day)
    write_csv(.x)
})



raw_transcripts_all_years <- input_files %>% map_dfr(~{
  vroom::vroom(.x)
})


raw_transcripts_all_years %>%
  mutate(quarter = as.Date(zoo::as.yearqtr(mostimportantdateutc))) %>%
  sample_n(10) %>%
  write_csv(path.expand("~/Downloads/sample_transcripts.csv"))
  write_csv(path.expand("~/Downloads/Raw Transcripts/aggregated_all_years.csv"))
  distinct(quarter)
  select(quarter,everything()) %>%
  group_by(quarter) %>%
  tally() %>%
  hchart('column',hcaes(quarter,n)) %>%
  hc_title(text = "Number of Earnings Calls by Quarter")

raw_transcripts_all_years %>%
  group_by(ticker,companyid,companyname) %>%
  tally() %>%
  arrange(desc(n))


read_csv(path.expand("~/Downloads/Raw Transcripts/aggregated_all_years.csv")) %>%
  mutate(quarter = as.Date(zoo::as.yearqtr(mostimportantdateutc)))

