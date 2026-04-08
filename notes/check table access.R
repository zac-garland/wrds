wrds_tables <- get_wrds_tables()
# wrds_tables%>% group_by(table_schema) %>% count() %>% View()
#

table_filts <- c("cboe","ciq","comp","compseg","crspm","crsp","wrdsapps")

check_access <- wrds_tables %>%
  filter(table_schema %in% table_filts) %>%
  mutate(
    has_access = map2(table_schema,table_name,~wharton_table(.x,.y)$error)
  )

table_access <- wrds_tables %>%
  filter(table_schema %in% table_filts) %>%
  anti_join(check_access %>% unnest())

table_access
