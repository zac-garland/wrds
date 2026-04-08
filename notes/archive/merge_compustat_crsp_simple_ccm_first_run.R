# Simple Compustat–CRSP merge (EDA-first) using CCM link table
# Goal: minimal objects, clear joins, all in-db until final collect()

library(tidyverse)
library(dbplyr)
library(lubridate)
devtools::load_all(".")

con <- connect_wharton()

# Parameters (make CUSIP optional)
cusip_target <- "93114210" # 8-digit CUSIP (no check digit); set to NULL to skip
cusip_target <- NULL
start_year <- 1980
end_year <- 2024
start_date <- as.Date("1979-01-01")
end_date <- as.Date("2024-12-31")

# Postgres-native fiscal-year start: datadate minus 11 months (in-database)
begfyr_expr <- dbplyr::sql("datadate - interval '11 months'")

# 0) Optional CUSIP -> GVKEYs filter (if provided)
if (!is.null(cusip_target)) {
  cusip_gvkey <- tbl(con, in_schema("comp", "names")) %>%
    filter(substr(cusip, 1, 8) == !!cusip_target) %>%
    distinct(gvkey)
} else {
  cusip_gvkey <- NULL
}

# 1) CCM link history: PERMNO/PERMCO <-> GVKEY (+ effective link dates)
ccm_link <- tbl(con, in_schema("crsp", "ccmxpf_lnkhist")) %>%
  filter(linktype %in% c("LU", "LC"), linkprim %in% c("P", "C")) %>%
  transmute(
    gvkey,
    permno = lpermno,
    permco = lpermco,
    linkdt,
    linkenddt = coalesce(linkenddt, as.Date("9999-12-31"))
  )

if (!is.null(cusip_gvkey)) {
  ccm_link <- ccm_link %>% inner_join(cusip_gvkey, by = "gvkey")
}

# 2) Compustat fundamentals (annual) + PDF filters + fiscal-year window markers
funda <- tbl(con, in_schema("comp", "funda")) %>%
  filter(fyear >= !!start_year, fyear <= !!end_year) %>%
  filter(indfmt == "INDL", datafmt == "STD", popsrc == "D", consol == "C") %>%
  filter(curcd == "USD", fic == "USA", between(exchg, 11, 19)) %>%
  filter(!between(sich, 6000, 6999), sich != 2834) %>%
  left_join(
    tbl(con, in_schema("comp", "names")) %>%
      select(gvkey, sic, naics, gsubind, gind),
    by = "gvkey"
  ) %>%
  mutate(gsector = substr(gind, 1, 2)) %>%
  {
    if (!is.null(cusip_gvkey)) {
      inner_join(., cusip_gvkey, by = "gvkey")
    } else {
      .
    }
  } %>%
  mutate(
    endfyr = datadate,
    begfyr = !!begfyr_expr,
    available_date = datadate + dbplyr::sql("interval '3 months'")
  ) %>%
  # Select only needed fields: dates, net income, R&D, names/tickers, sector
  # (conm, tic, cusip, cik already in funda, so no need to join from names)
  select(
    gvkey,
    datadate,
    fyear,
    fyr,
    apdedate,
    fdate,
    pdate,
    conm,
    tic,
    cusip,
    cik,
    ni,
    xrd,
    gsector,
    endfyr,
    begfyr,
    available_date
  )

# 3) CRSP monthly returns (restricted date range) + PDF filters
msf <- tbl(con, in_schema("crsp", "msf")) %>%
  select(permno, date, ret, prc, vol, shrout) %>%
  filter(date >= !!start_date, date <= !!end_date) %>%
  filter(!is.na(ret)) %>%
  inner_join(
    tbl(con, in_schema("crsp", "msenames")) %>%
      select(permno, namedt, nameendt, siccd),
    by = "permno"
  ) %>%
  filter(
    date >= namedt,
    date <= coalesce(nameendt, as.Date("9999-12-31")), !between(siccd, 6000, 6999),
    siccd != 2834
  ) %>%
  mutate(mktcap = abs(prc * shrout)) %>%
  select(permno, date, ret, prc, vol, mktcap)

# 3b) Apply CCM effective-date validity at the month level, then collapse any overlaps
# (avoids unintended many-to-many from overlapping link windows)
link_msf <- msf %>%
  inner_join(ccm_link, by = "permno") %>%
  filter(date >= linkdt, date <= linkenddt) %>%
  group_by(permno, date) %>%
  slice_max(linkdt, n = 1, with_ties = FALSE) %>%
  ungroup()

# 4) Merge logic (backtesting-aware: efficient "as of" join)
# Collect unique dates first to enable more efficient join strategy
crsp_dates <- link_msf %>%
  distinct(gvkey, permno, date) %>%
  collect()

# For each CRSP date, get latest funda where available_date <= date
# This avoids cartesian join by working with collected dates
merged <- crsp_dates %>%
  inner_join(funda %>%
    filter(available_date <= !!end_date) %>%
    collect(), by = "gvkey") %>%
  filter(date >= available_date) %>%
  group_by(gvkey, permno, date) %>%
  slice_max(datadate, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  inner_join(
    link_msf %>%
      select(gvkey, permno, date, ret, prc, vol, mktcap) %>%
      rename(ret_crsp = ret, prc_crsp = prc) %>%
      collect(),
    by = c("gvkey", "permno", "date")
  ) %>%
  select(-begfyr, -endfyr, -available_date) %>%
  # Apply remaining CRSP filters (letter returns, extreme returns)
  filter(!grepl("[A-Za-z]", ret_crsp), as.numeric(ret_crsp) >= -100) %>%
  {
    nm <- funda_name_mapping()
    rename_with(., ~ dplyr::coalesce(nm$snake_name[match(.x, nm$variable_name)], .x))
  } %>%
  add_gics_names()

merged


merged %>%
  select(
    gvkey,
    permno,
    date,
    datadate,
    company_name,
    ticker,
    sector = sector_name,
    ret_crsp,
    mktcap,
    net_income_loss,
    research_and_development_expense
  ) %>%
  filter(ret_crsp != 0) %>%
  arrange(permno, date) %>%
  group_by(permno) %>%
  mutate(mktcap = lag(mktcap)) %>%
  mutate(
    date = ceiling_date(date, "month") - 1
  ) %>%
  mutate(research_not = case_when(
    is.na(research_and_development_expense) ~ "No RND",
    TRUE ~ "RND"
  )) %>%
  group_by(date, research_not) %>%
  mutate(weight = mktcap / sum(mktcap, na.rm = TRUE)) %>%
  mutate(weighted_ret = weight * ret_crsp) %>%
  group_by(date, research_not) %>%
  summarize(
    weighted_ret = sum(weighted_ret, na.rm = TRUE),
    average_ret = mean(ret_crsp, na.rm = TRUE)
  ) %>%
  arrange(research_not, date) %>%
  gather(key, value, -(1:2)) %>%
  group_by(research_not, key) %>%
  mutate(cumret = with_order(date, cumprod, 1 + value)) %>%
  unite(research_not, research_not, key) %>%
  hchart("line", hcaes(date, cumret, group = research_not))

library(zgtools)


merged %>%
  group_by(permno) %>%
  mutate(
    invest_criteria = case_when(
      prc_crsp > 1 & lag(ret_crsp) != 0 & !is.na(sector_name) & vol > 10000 & !is.na(lag(ret_crsp)) ~ 1, TRUE ~ 0
    ),
    roll_criteria = cumsum(invest_criteria)
  ) %>%
  filter(
    roll_criteria >= 12
  )%>%
  select(
    gvkey,
    permno,
    date,
    datadate,
    company_name,
    ticker,
    sector = sector_name,
    ret_crsp,
    mktcap,
    net_income_loss,
    research_and_development_expense
  ) %>%
  filter(ret_crsp != 0) %>%
  arrange(permno, date) %>%
  group_by(permno) %>%
  mutate(mktcap = lag(mktcap)) %>%
  mutate(
    date = ceiling_date(date, "month") - 1
  ) %>%
  mutate(research_not = case_when(
    is.na(research_and_development_expense) ~ "No RND",
    TRUE ~ "RND"
  )) %>%
  group_by(date, research_not) %>%
  mutate(weight = mktcap / sum(mktcap, na.rm = TRUE)) %>%
  mutate(weighted_ret = weight * ret_crsp) %>%
  group_by(date, research_not) %>%
  summarize(
    weighted_ret = sum(weighted_ret, na.rm = TRUE),
    average_ret = mean(ret_crsp, na.rm = TRUE)
  ) %>%
  arrange(research_not, date) %>%
  gather(key, value, -(1:2)) %>%
  group_by(research_not, key) %>%
  mutate(cumret = with_order(date, cumprod, 1 + value)) %>%
  unite(research_not, research_not, key) %>%
  hchart("line", hcaes(date, cumret, group = research_not))
