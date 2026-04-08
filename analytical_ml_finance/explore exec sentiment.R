detail <- wharton_table("ciq", "wrds_transcript_detail")$result %>% filter(transcriptid == 3294948) %>% collect()
person <- wharton_table("ciq", "wrds_transcript_person")$result %>% filter(transcriptid == 3294948) %>% collect()
component <- wharton_table("ciq", "ciqtranscriptcomponent")$result %>% filter(transcriptid == 3294948) %>% collect()


unique(person$proid) %>% na.omit() -> proids

as.character(proids)

sample_proids <- wharton_table('ciq','ciqprofessional')$result %>% filter(proid %in% local(as.double(proids))) %>% collect()

sample_proids


sp500_transcripts <- earnings_calls_raw %>% inner_join(sp500) %>% filter(between(mostimportantdateutc,start,ending))


# tbl(con,in_schema(''))

component

person

con <- connect_wharton()

sp500_transcripts <- read_csv('exec_transcripts/sp500_transcripts.csv')$transcriptid
input_vec <- c(1, 3107, 6213, 9319, 12425, 15531, 18637, 21743, 24849, 27955)



# walk2(input_vec,lead(input_vec),~{
#   if(.x != 27955){
#     filt_transcripts <- sp500_transcripts[.x:.y]
#
#     tbl(con,in_schema('ciq','wrds_transcript_person')) %>%
#       filter(speakertypeid == as.integer(2)) %>%
#       filter(transcriptid %in% local(filt_transcripts)) %>%
#       filter(!is.na(proid)) %>%
#       select(-all_of(c("companyofperson","componenttextpreview","word_count"))) %>%
#       inner_join(tbl(con,in_schema('ciq','ciqprofessional')) %>% select(proid,title,prorank)) %>%
#       inner_join(tbl(con,in_schema('ciq','ciqtranscriptcomponent'))) %>%
#       collect() %>%
#       write_csv(glue::glue('exec_transcripts/{.x}.csv'))
#     print(glue::glue('wrote {.x}'))
#   }
# })


earnings_calls_raw





output_dat <- c(1, 3107, 6213, 9319, 12425, 15531, 18637, 21743, 24849, 27955)[1:9] %>%
  map_dfr(~{
    read_csv(here::here('exec_transcripts',paste0(.x,".csv")))
  })

output_dat <- read_csv('exec_transcripts/aggretated_exec.csv')


earnings_calls_raw <- vroom::vroom(
  path.expand("~/Downloads/Raw Transcripts/aggregated_all_years.csv")
) |>
  mutate(
    quarter        = as_date(zoo::as.yearqtr(mostimportantdateutc)),
    fiscal_year    = year(quarter),
    fiscal_quarter = paste0("Q", quarter(quarter))
  )

output_dat %>%
  mutate(title = str_to_lower(title,"")) %>%
  mutate(title = str_replace_all(title,"former","") %>% str_remove_all("executive") %>%str_trim() %>% str_squish()) %>%
  mutate(title = case_when(
    str_detect(title,"ceo") | str_detect(title,"president") | str_detect(title,"chief executive officer")  | str_detect(title,"chief officer") | str_detect(title,"chairman") ~  "ceo",
    str_detect(title,"cfo") | str_detect(title,"chief financial officer") ~  "cfo",
    str_detect(title,"coo") | str_detect(title,"chief operating officer") ~  "cfo",
    TRUE ~ "other"
  )) %>%
  left_join(
    earnings_calls_raw %>%
  select(transcriptid,mostimportantdateutc,companyname,ticker,close_to_open_return,quarter)
  ) %>%
  filter(ticker == "MA") %>%
  filter(transcriptid == "84312") %>%
  clipr::write_clip()



earnings_call_sample <- vroom::vroom(
  path.expand("~/Downloads/Raw Transcripts/aggregated_all_years.csv")
) %>% filter(transcriptid == 3294948)


component %>% mutate(word_count = str_count(componenttext,"\\S+")) %>% arrange(componentorder) %>% mutate(to = cumsum(word_count+1)-1, from = to-word_count+1) -> from_to_indices



transcript_to_turn <- function(transcript,word_counts){

  all_words <- unlist(str_split(str_trim(str_squish(transcript)) %>% str_replace_all("\\.\\.\\.","\\s"),"\\s+"))
  groups <- rep(seq_along(word_counts),times = word_counts)
  split(all_words[1:length(groups)],groups) %>%
    map_chr(~paste(.x,collapse = " ")) %>%
    unname()

}



component %>%
  arrange(componentorder) %>%
  bind_cols(
    transcript_to_turn(earnings_call_sample$full_transcript_text,component %>% arrange(componentorder) %>%  mutate(word_length = str_count(componenttext,"\\S+")) %>% pull(word_length)) %>%
      tibble(attempt_component_text = .)
  ) %>%
  mutate(equal = str_sub(componenttext,1,10) == str_sub(attempt_component_text,1,10)) %>%
  print(n = nrow(.))


person

con <- connect_wharton()

tbl(con,dbplyr::in_schema('ciq','wrds_transcript_person'))

out_test <- tbl(con, dbplyr::in_schema("ciq", "ciqtranscriptcomponent")) %>%
  filter(transcriptid == 3294948) %>%
  mutate(
    word_length = sql(
      "COALESCE(array_length(regexp_split_to_array(trim(componenttext), '\\s+'), 1), 0)"
    )
  ) %>%
  select(transcriptcomponentid, transcriptid, componentorder, word_length) %>%
  collect()

from_to_indices %>% bind_cols(out_test %>% arrange(componentorder)) %>% select(word_count,word_length) %>% mutate(test = word_count == word_length) %>% print(n = nrow(.))

COALESCE(array_length(regexp_split_to_array(trim(componenttext), '\s+'), 1), 0)
