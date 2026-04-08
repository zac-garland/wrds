wrds_tables <- get_wrds_tables()

wrds_tables %>%
  filter(table_schema == "trdstrm") %>%
  print(n = nrow(.))


fut_series <- wharton_table("trdstrm", "wrds_fut_series")$result

fut_info <- wharton_table("trdstrm", "wrds_cseries_info")$result

non_dead <- fut_info %>%
  filter(!calcseriesname %like% "%DEAD%") %>%
  filter(isocurrcode == "USD", rollmethodcode == 0, positionfwdcode == 0)


fut_series %>%
  inner_join(non_dead) %>%
  group_by(calcseriescode, calcseriesname) %>%
  summarize(across(c(volume, settlement), ~ mean(., na.rm = TRUE))) %>%
  collect() -> vol_fut

vol_fut %>%
  filter(!is.na(volume)) %>%
  arrange(desc(volume)) %>%
  left_join(non_dead %>% collect()) %>%
  distinct(calcseriescode, calcseriesname, volume) %>%
  clipr::write_clip("table")
print(n = nrow(.))
print(n = nrow(.))


tibble::tribble(
  ~calcseriescode, ~common_name, ~calcseriesname, ~volume,
  "15907", "5Y Treasury Note", "CBT-5 YEAR US T-NOTE COMP. CONT.", 1690375.89328537,
  "26854", "3-Month SOFR", "CME-3 MONTH SOFR CONT", 1559439.70842105,
  "27695", "Micro Nasdaq-100", "CBT-MICRO NASDAQ 100 E-MINI CONT", 1444759.34504792,
  "8539", "Micro E-mini S&P 500", "CME-MINI S&P 500 INDEX CONT.", 1380193.88465001,
  "4976", "10Y Treasury Note", "ECBOT-10 YEAR US T-NOTE CONT.", 1150489.58880655,
  "22179", "2Y Treasury Note", "CBT-2 YEAR US T-NOTE COMP. CONTINUOUS", 975866.398880895,
  "8290", "Brent Crude Oil (Moscow Exchange)", "RTS-BRENT CRUDE OIL CONTINUOUS", 909086.717208549,
  "26695", "Ultra 10Y Treasury Note", "CBT-ULTRA 10-YEAR US T-NOTE CONT", 658551.26351622,
  "974", "RTS Index (Russian Equities)", "RTS-RTS INDEX CONTINUOUS", 593136.895640908,
  "20896", "30Y Treasury Bond", "CBT-30 YR US T-BOND COMP CONTINUOUS", 513474.363527534,
  "9761", "WTI Crude Oil", "NYM-LIGHT CRUDE OIL CONTINUOUS", 427724.152919872,
  "15924", "Ultra T-Bond", "CBOT-ULTRA T-BOND COMP. CONT.", 399310.352422907,
  "5888", "Natural Gas", "NYM-NATURAL GAS CONTINUOUS", 320129.929944948,
  "2513", "FTSE China A50 Index", "SGX DT-FTSE CHINA A50 CONTINUOUS", 223360.514559689,
  "26505", "E-mini Russell 2000", "CME-E-MINI RUSSELL 2000 C CONT", 205495.162790698,
  "769", "Gas Oil (Heating Oil)", "ICE-GAS OIL CONTINUOUS", 203594.359359709,
  "13396", "VIX (Volatility Index)", "CFE-VIX INDEX CONTINUOUS", 198115.502306509,
  "16249", "Euro/USD", "CME-EURO COMP. CONTINUOUS", 181080.074407337,
  "11895", "Aluminum", "LME-ALUMINIUM CONTINUOUS", 152480.145203064,
  "11627", "RBOB Gasoline", "NYMEX-RBOB GASOLINE CONTINUOUS", 144418.978509464,
  "27697", "Micro E-mini Dow Jones", "CBT-MICRO E-MINI DJIA CONT", 127194.843045113,
  "1447", "Heating Oil (ULSD)", "NYM-NY HARBOR ULSD CONTINUOUS", 117716.513500884,
  "8025", "Gold", "CMX-GOLD 100 OZ CONTINUOUS", 106304.27192481,
  "23239", "Soybeans", "CBT-SOYBEANS COMP. CONT.", 102110.026545744,
  "16518", "MSCI Emerging Markets Index", "NYL-MSCI EMER MARKET MINI CONT.", 89087.6589151011,
  "29470", "Ethereum Micro", "CME-MICRO ETHER INDEX CONT", 81825.5741626794,
  "5937", "Copper", "LME-COPPER CONTINUOUS", 65165.4049208363,
  "3301", "Silver (Moscow Exchange)", "RTS-SILVER CONTINUOUS", 61679.4503938485,
  "10437", "Zinc", "LME-ZINC CONTINUOUS", 60392.8352986555,
  "27418", "Taiwan Index", "SGX DT-TAIWAN INDEX CONT", 58645.4275226717,
  "29483", "Bitcoin Micro", "CME-MICRO BITCOIN INDEX CONT", 53030.6519138756,
  "21350", "Wheat", "CBT-WHEAT COMPOSITE FUTURES CONT.", 51492.6939537779,
  "24593", "Indian Rupee/USD", "SGX-INR/USD CONT", 46154.4433925307,
  "29297", "Nifty 50 (Indian Equities)", "NXI-NIFTY 50 INDEX CONT", 39636.2711609234,
  "1514", "Nickel", "LME-NICKEL CONTINUOUS", 37317.7757220964,
  "16506", "MSCI EAFE (Developed Markets ex-US)", "NYL-MSCI EAFE MINI CONTINUOUS", 27583.0163918666,
  "3668", "Lead", "LME-LEAD CONTINUOUS", 26587.3526812835,
  "490", "Robusta Coffee", "LIFFE-ROBUSTA COFFEE REVISED CONT.", 14510.7902110225,
  "12938", "US Dollar Index", "FINEX-US DOLLAR INDEX CONT.", 12138.2504511711,
  "13466", "E-mini S&P 400 MidCap", "CME-EMINI S&P 400 MIDCAP CONTINUOU", 11593.8258785942
)


futures_contracts

series_codes <- as.double(unique(futures_contracts$calcseriescode))

fut_data <- fut_series %>%
  filter(calcseriescode %in% series_codes) %>%
  rename_with(janitor::make_clean_names) %>%
  distinct(calcseriescode,date,open,high,low,volume,settlement,openinterest) %>%
  collect()


fut_data %>%
  left_join(futures_contracts %>% select(calcseriescode,common_name) %>% mutate(calcseriescode = as.double(calcseriescode))) %>%
  arrange(common_name,date) %>%
  group_by(common_name) %>%
  split(.$common_name) %>%
  imap(~{
    hchart(.x,"line",hcaes(date,open)) %>%
      hc_title(text = .y)
  }) %>% hw_grid()


library(highcharter)
futures_contracts %>%
  pull(volume) %>%
  hchart()

tema <- function(x,n = 10){
  ema1 <- EMA(x, n = n)
  ema2 <- EMA(ema1, n = n)
  ema3 <- EMA(ema2, n = n)
  3 * ema1 - 3 * ema2 + ema3
}


fut_data %>%
  left_join(futures_contracts %>% select(calcseriescode,common_name) %>%
              mutate(calcseriescode = as.double(calcseriescode))) %>%
  arrange(common_name,date) %>%  group_by(common_name) %>% filter(!is.na(open)) %>%
  mutate(open = log(open)-lag(log(open),10)) %>%
  mutate(tema = tema(open)) %>%
  filter(date >= as_date("2023-05-01")) %>%
  group_by(common_name) %>%
  split(.$common_name) %>%
  imap(~{
    hchart(.x,"line",hcaes(date,open),name = "tema") %>%
      # hc_add_series(.x,"line",hcaes(date,open),name = "open") %>%
      hc_title(text = .y)
  }) %>% hw_grid(ncol = 2)

intraday_series <- read_csv(path.expand("~/Downloads/MASTER_MARKET_DATA_v14.csv"))

intraday_series %>%
  group_by(name,year = year(dt)) %>%
  summarize(dt_diff = mean(dt-lag(dt),na.rm = TRUE)) %>%
  arrange(dt_diff) %>%
  print(n = nrow(.))

filt <- c('Nasdaq 100 ETF','S&P 500 ETF','Russell 2000 ETF','Silver ETF','Gold ETF')
{
  filt_date <- unique(intraday_series$dt) %>% sample(1) %>% as_date()

  intraday_series %>% filter(name %in% filt) %>%
    filter(as_date(dt) == filt_date) %>%
    filter(between(hour(dt),9,18)) %>%
    split(.$name) %>%
    imap(~{
      hchart(.x,"line",hcaes(datetime_to_timestamp(dt),o)) %>%
        hc_title(text = paste(.y,unique(as_date(.x$dt)))) %>%
        hc_xAxis(type = "datetime")
    }) %>%
    hw_grid()

}
