# Function to get CRSP monthly panel with security descriptors
get_crsp_monthly_panel <- function(con, start_date, end_date) {
  # Get years in range
  start_year <- year(as.Date(start_date))
  end_year <- year(as.Date(end_date))
  years <- start_year:end_year


  # Determine chunk boundaries
  chunk_start <- start_date
  chunk_end <- end_date

  # Query using dbplyr instead of raw SQL
  df <- tbl(con, in_schema("crsp", "msf")) %>%
    inner_join(
      tbl(con, in_schema("crsp", "msenames")),
      by = "permno"
    ) %>%
    filter(
      date >= !!chunk_start,
      date <= !!chunk_end,
      date >= namedt,
      date <= coalesce(nameendt, as.Date("9999-12-31")),
      shrcd %in% c(10, 11),
      exchcd %in% c(1, 2, 3),
      !is.na(prc),
      !is.na(shrout)
    ) %>%
    select(date,ticker,comnam, permno, prc, shrout, ret, shrcd, exchcd) %>%
    arrange(date, permno) %>%
    collect()

  # Combine all years
  # df <- bind_rows(frames)

  # Clean and process data
  df <- df %>%
    mutate(
      date = as.Date(date),
      prc = abs(as.numeric(prc)), # Absolute value for price
      shrout = as.numeric(shrout) * 1000, # Convert to shares
      ret = as.numeric(ret),
      mktcap = prc * shrout
    )

  return(df)
}

# Function to get delisting returns
get_delistings <- function(con, start_date, end_date) {
  delist_data <- tbl(con, in_schema("crsp", "msedelist")) %>%
    filter(
      dlstdt >= !!start_date,
      dlstdt <= !!end_date,
      !is.na(dlret)
    ) %>%
    select(permno, date = dlstdt, dlret) %>%
    collect() %>%
    mutate(
      date = as.Date(date),
      dlret = as.numeric(dlret)
    )

  return(delist_data)
}

# Function to add effective returns
add_effective_returns <- function(panel, delist) {
  out <- panel %>%
    left_join(delist, by = c("permno", "date")) %>%
    mutate(
      ret_eff = (1 + coalesce(ret, 0)) * (1 + coalesce(dlret, 0)) - 1
    )

  return(out)
}

panel <- get_crsp_monthly_panel(con, START_DATE, END_DATE)
cat("Rows:", nrow(panel), "\n")

delist <- get_delistings(con, START_DATE, END_DATE)
panel <- add_effective_returns(panel, delist)

weight_panel <- panel %>%
  mutate(date = date %m-% months(1)) %>%
  group_by(date) %>%
  add_tally() %>%
  mutate(
    price_weight = prc/sum(prc,na.rm=TRUE),
    value_weight = mktcap/sum(mktcap,na.rm=TRUE),
    eq_weight = 1/n
  ) %>%
  select(date,ticker,comnam,permno,contains("weight")) %>%
  distinct()


custom_indexes <- weight_panel %>%
  inner_join(panel %>% select(date,ticker,comnam,permno,ret_eff)) %>%
  mutate(across(contains("weight"),~.*ret_eff)) %>%
  group_by(date) %>%
  summarize(across(contains("weight"),~sum(.,na.rm=TRUE))) %>%
  add_row(date = min(.$date,na.rm=TRUE) %m-% months(1),.before = 1) %>%
  replace_na(list(price_weight = 0, value_weight = 0, eq_weight = 0)) %>%
  mutate(across(contains("weight"),~cumprod(1+.)*100))

custom_indexes %>%
  gather(key,value,-date) %>%
  hchart("line",hcaes(date,value,group = key))
