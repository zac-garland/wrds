devtools::load_all()
wrds_tables <- get_wrds_tables()

wrds_tables

msf_tbl <- wharton_table("crsp","msf")$result
msename_tbl <- wharton_table("crsp","msenames")$result

msf_names <- msf_tbl %>% head(1) %>% collect() %>% names()
msf_names <- msf_names[-which(msf_names == "permno")]




full_sec_rets <- msf_tbl %>%
  left_join(
    msename_tbl %>% select(-any_of(msf_names)),by = "permno"
  ) %>%
  collect()

full_sec_rets %>% filter(date <= nameendt) %>% write_csv('full_sec_rets.csv')

full_sec_rets %>% filter(!shrcd %in% c(10:11)) %>%
  filter(date == max(date,na.rm=TRUE)) %>%
  distinct(ticker,comnam) %>%
  View()
  glimpse()
