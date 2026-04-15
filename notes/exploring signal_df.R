library(zgtools)
signal_df <- read_csv("final_project/signal_df.csv") %>%
  select(
  ticker,
  quarter_str,
  close_to_open_return,
  novelty_score,
  salience_score,
  trailing_unique_analysts_90d,
  llm_intent_pass,
  cross_market_freq,
  penalty_mult,
  adjusted_novelty,
  llm_impact_score
)

signal_df %>%
  mutate(adjusted_novelty = novelty_score*sqrt(salience_score)) %>%
  select(ticker,quarter_str,close_to_open_return,adjusted_novelty,novelty_score,llm_impact_score) %>%
  mutate(signal = adjusted_novelty*llm_impact_score) %>%
  filter(llm_impact_score!= 0,novelty_score > .3,abs(signal) > 0.5) %>%
  group_by(ticker,quarter_str) %>%
  summarize(across(close_to_open_return:signal,~mean(.,na.rm=TRUE))) %>%
  ungroup() %>%
  lm(close_to_open_return ~ signal,data = .) %>% summary()
  hchart('scatter',hcaes(signal,close_to_open_return))


speaker_indices <- read_csv("final_project/transcript_speaker_indices.csv")

chunk_to_component <- read_csv("final_project/chunk_to_component.csv")

speaker_question_start <- speaker_indices %>% group_by(transcriptid) %>%
  filter(str_detect(str_to_lower(transcriptcomponenttypename),"question")) %>%
  arrange(transcriptid,componentorder) %>%
  slice(1)

filt_signal <- chunk_to_component %>%
  # select(transcriptid,chunk_index,componentorder) %>%
  # filter(transcriptid == 92362) %>%
  left_join(speaker_question_start,join_by(transcriptid,componentorder > componentorder)) %>%
  filter(!is.na(componentorder.y)) %>%
  select(transcriptid,chunk_index)


filt_signal


signal_df %>%
  inner_join(filt_signal) %>%
  mutate(adjusted_novelty = novelty_score * sqrt(salience_score) * llm_impact_score) %>% mutate(filt_test = dense_rank(adjusted_novelty)) %>% filter(filt_test <= 10 | filt_test >= max(filt_test)-10) %>% print(n = nrow(.)) %>% pull(sentence) %>% writeLines()
