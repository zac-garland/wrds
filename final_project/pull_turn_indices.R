con <- connect_wharton()

sp500_transcripts <- read_csv('exec_transcripts/sp500_transcripts.csv')$transcriptid
input_vec <- c(1, 3107, 6213, 9319, 12425, 15531, 18637, 21743, 24849, 27955)

walk2(input_vec,lead(input_vec),~{
  if(.x != 27955){
    filt_transcripts <- sp500_transcripts[.x:.y]

    tbl(con,in_schema('ciq','wrds_transcript_person')) %>%
      filter(speakertypeid == as.integer(2)) %>%
      filter(transcriptid %in% local(filt_transcripts)) %>%
      filter(!is.na(proid)) %>%
      select(-all_of(c("companyofperson","componenttextpreview","word_count"))) %>%
      inner_join(tbl(con,in_schema('ciq','ciqprofessional')) %>% select(proid,title,prorank)) %>%
      inner_join(
        tbl(con, dbplyr::in_schema("ciq", "ciqtranscriptcomponent")) %>%
          mutate(
            word_length = sql(
              "COALESCE(array_length(regexp_split_to_array(trim(componenttext), '\\s+'), 1), 0)"
            )
          ) %>%
          select(transcriptcomponentid, transcriptid, componentorder, word_length)

      ) %>%
      collect() %>%
      write_csv(glue::glue('exec_transcripts/{.x}_indices.csv'))
    print(glue::glue('wrote {.x}'))
  }
})


sp500_transcripts <- read_csv('final_project/FINAL.csv')$transcriptid
input_vec <- seq(1,length(sp500_transcripts),by = 5000)

input_vec[[length(input_vec)]] <- length(sp500_transcripts)



walk2(input_vec,lead(input_vec),~{
  if(.x != input_vec[[length(input_vec)]]){
    filt_transcripts <- sp500_transcripts[.x:.y]


    tbl(con, dbplyr::in_schema("ciq", "ciqtranscriptcomponent")) %>%
      filter(transcriptid %in% local(filt_transcripts)) %>%
      mutate(
        word_length = sql(
          "COALESCE(array_length(regexp_split_to_array(trim(componenttext), '\\s+'), 1), 0)"
        )
      ) %>%
      select(transcriptcomponentid, transcriptid, componentorder, word_length) %>%
      left_join(
        tbl(con,in_schema('ciq','wrds_transcript_person')) %>%
          select(-all_of(c("companyofperson","componenttextpreview","word_count")))
      ) %>%
      left_join(tbl(con,in_schema('ciq','ciqprofessional')) %>% select(proid,title,prorank)) %>%
      collect() %>%
      write_csv(glue::glue('exec_transcripts/{.x}_indices_all.csv'))
    print(glue::glue('wrote {.x}'))
  }
})


agg_indices <- list.files("exec_transcripts",pattern = "indices_all",full.names = TRUE) %>%
  map_dfr(~{
    read_csv(.x)
  })

agg_indices %>%
  select(transcriptid,transcriptcomponentid,componentorder,transcriptcomponenttypename,transcriptpersonname,speakertypename,transcriptcomponenttypename,prorank,word_count = word_length) %>%
  arrange(transcriptid,componentorder) %>%
  write_csv('final_project/transcript_speaker_indices.csv')



final_agg <- read_csv('final_project/FINAL.csv')

final_agg %>% filter(ticker == "MA") %>% tail()



