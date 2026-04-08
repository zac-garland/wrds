if (!require("pacman", quietly = TRUE)) {
  install.packages("pacman")
}

pacman::p_load(tidyverse, RPostgres, highcharter, lubridate, dbplyr, getPass)

setup_wrds <- function() {
  if (!require("getPass", quietly = TRUE)) install.packages("getPass")
  if (!require("RPostgres", quietly = TRUE)) install.packages("RPostgres")

  username <- readline("WRDS username: ")
  password <- gsub("([:\\\\])", "\\\\\\1", getPass::getPass("WRDS password: "))

  if (Sys.info()["sysname"] == "Windows") {
    dir.create(file.path(Sys.getenv("APPDATA"), "postgresql"), showWarnings = FALSE)
    writeLines(
      paste0("wrds-pgdata.wharton.upenn.edu:9737:wrds:", username, ":", password),
      file.path(Sys.getenv("APPDATA"), "postgresql", "pgpass.conf")
    )
    home <- Sys.getenv("USERPROFILE")
  } else {
    pgpass <- file.path(Sys.getenv("HOME"), ".pgpass")
    writeLines(paste0("wrds-pgdata.wharton.upenn.edu:9737:wrds:", username, ":", password), pgpass)
    Sys.chmod(pgpass, "0600")
    home <- Sys.getenv("HOME")
  }

  rprofile <- file.path(home, ".Rprofile")
  writeLines(c("library(RPostgres)", paste0("wrds <- dbConnect(Postgres(), host='wrds-pgdata.wharton.upenn.edu', port=9737, dbname='wrds', sslmode='require', user='", username, "')")), rprofile)

  if (Sys.info()["sysname"] != "Windows") Sys.chmod(rprofile, "0600")
  cat("Setup complete. Restart R.\n")
}

setup_wrds()

START_DATE <- "2000-01-01"
END_DATE <- Sys.Date()


connect_wharton <- function() {
  library(RPostgres)
  dbConnect(Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    port = 9737,
    dbname = "wrds",
    sslmode = "require",
    user = "zkg232"
  )
}

check_wharton_connection <- function() {
  if (exists("con", where = 1)) { # 1 refers to global environment
    connection <- con
  } else {
    connection <- connect_wharton()
    con <<- connection
  }
  con
}

get_wrds_tables <- function() {
  con <- check_wharton_connection()
  tbl(con, dbplyr::in_schema("information_schema", "tables")) %>%
    filter(table_type == "VIEW" | table_type == "FOREIGN TABLE") %>%
    distinct(table_schema, table_name) %>%
    collect()
}

wharton_table <- function(db, name) {
  con <- check_wharton_connection()

  safe_tbl <- safely(tbl)

  res <- safe_tbl(con, dbplyr::in_schema(db, name))

  if (length(res$error) > 0) {
    res$error <- res$error$parent$message %>% str_trim()
  }
  res
}


# Function to get CRSP monthly panel with security descriptors
get_crsp_monthly_panel <- function(con = connect_wharton(), start_date, end_date) {
  # Query using dbplyr instead of raw SQL
  df <- tbl(con, in_schema("crsp", "msf")) %>%
    inner_join(
      tbl(con, in_schema("crsp", "msenames")),
      by = "permno"
    ) %>%
    filter(
      date >= !!start_date,
      date <= !!end_date,
      date >= namedt,
      date <= coalesce(nameendt, as.Date("9999-12-31")),
      shrcd %in% c(10, 11),
      exchcd %in% c(1, 2, 3),
      !is.na(prc),
      !is.na(shrout)
    ) %>%
    select(date, ticker, comnam, permno, prc, shrout, ret, shrcd, exchcd) %>%
    arrange(date, permno) %>%
    collect()

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
get_delistings <- function(con = check_wharton_connection(), start_date, end_date) {
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

panel <- get_crsp_monthly_panel(con = check_wharton_connection(), START_DATE, END_DATE)
cat("Rows:", nrow(panel), "\n")

delist <- get_delistings(con = check_wharton_connection(), START_DATE, END_DATE)
panel <- add_effective_returns(panel, delist)

weight_panel <- panel %>%
  mutate(date = date %m-% months(1)) %>%
  group_by(date) %>%
  mutate(rank = dense_rank(desc(mktcap))) %>%
  filter(rank <= 500) %>%
  add_tally() %>%
  mutate(
    price_weight = prc / sum(prc, na.rm = TRUE),
    value_weight = mktcap / sum(mktcap, na.rm = TRUE),
    eq_weight = 1 / n
  ) %>%
  select(date, ticker, comnam, permno, contains("weight")) %>%
  distinct()

weight_panel %>%
  filter(!is.na(price_weight)) %>%
  ungroup() %>%
  mutate(avg_weight = rowMeans(select(., contains("weight")),na.rm = TRUE)) %>%
  arrange(desc(avg_weight)) %>%
  group_by(comnam) %>%
  summarize(across(contains('weight'),~mean(.,na.rm=TRUE))) %>%
  arrange(desc(avg_weight)) %>%
  slice(1:20)


custom_indexes <- weight_panel %>%
  inner_join(panel %>% select(date, ticker, comnam, permno, ret_eff)) %>%
  mutate(across(contains("weight"), ~ . * ret_eff)) %>%
  group_by(date) %>%
  summarize(across(contains("weight"), ~ sum(., na.rm = TRUE))) %>%
  add_row(date = min(.$date, na.rm = TRUE) %m-% months(1), .before = 1) %>%
  replace_na(list(price_weight = 0, value_weight = 0, eq_weight = 0)) %>%
  mutate(across(contains("weight"), ~ cumprod(1 + .) * 100))

custom_indexes %>%
  gather(key, value, -date) %>%
  hchart("line", hcaes(date, value, group = key))

