# ============================================================================
# DISCLAIMER: This is an R file, I figured if there was any assignment to use
# R in, it was this one - as it's not a group project. After a few months
# of coding in python, I'm fairly convinced there's not a significant
# difference between the two, it's primarily a preference. If there is an issue
# please feel free to connect with me directly.
# Warm Regards
# Zac Garland
# Note - While I ordinarily work in a package format
# ref:  https://github.com/zac-garland/fmpr
# https://github.com/zac-garland/zgtools
# This workflow has been made into a reprex (reproducible example)
# It should run in R / Rstudio, if you run into issues, please feel free to
# ask me, I love talking R!
# ============================================================================


# ============================================================================
# Compustat-CRSP Merge: Self-Contained Reprex (Final Cleaned Version)
# ============================================================================
#
# This script merges annual Compustat fundamentals with monthly CRSP returns
# using the CCM linking table for accurate historical security matching.
#
# Features:
# - CCM linking table for PERMNO <-> GVKEY matching
# - Backtesting-aware: 3-month data availability lag
# - Delisting returns and effective returns calculation
# - PDF filters: exchange codes, share codes, SIC exclusions
# - Snake_case column renaming
# - GICS sector name mapping
#
# Requirements:
# - WRDS account credentials (update username in connect_wharton())
# - Internet connection for WRDS database access
#
# ============================================================================

# ============================================================================
# SETUP: Install and load packages
# ============================================================================

required_packages <- c("tidyverse", "dbplyr", "lubridate", "RPostgres", "devtools",
                       "knitr", "kableExtra", "htmltools", "purrr")
new_packages <- required_packages[!(required_packages %in% installed.packages()[, "Package"])]
if (length(new_packages)) install.packages(new_packages)

if (!requireNamespace("zgtools", quietly = TRUE)) {
  devtools::install_github("zac-garland/zgtools")
}

library(tidyverse)
library(dbplyr)
library(lubridate)
library(RPostgres)
library(knitr)
library(kableExtra)
library(htmltools)
library(purrr)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

connect_wharton <- function(username = "zkg232") {
  dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    port = 9737,
    dbname = "wrds",
    sslmode = "require",
    user = username
  )
}

funda_name_mapping <- function() {
  tribble(
    ~variable_name, ~snake_name,
    "gvkey", "gvkey",
    "datadate", "report_date",
    "fyear", "fiscal_year",
    "fyr", "fiscal_year_end_month",
    "apdedate", "actual_period_end_date",
    "fdate", "final_date",
    "pdate", "preliminary_date",
    "conm", "company_name",
    "tic", "ticker",
    "cusip", "cusip",
    "cik", "cik",
    "ni", "net_income_loss",
    "xrd", "research_and_development_expense",
    "gsector", "gic_sectors",
    "permno", "permno",
    "date", "date",
    "ret_crsp", "ret_crsp",
    "prc_crsp", "prc_crsp",
    "vol", "vol",
    "mktcap", "mktcap",
    "ret_eff", "ret_eff"
  )
}

gics_mapping <- function() {
  tribble(
    ~gic_sectors, ~sector_name,
    "10", "Energy", "15", "Materials", "20", "Industrials",
    "25", "Consumer Discretionary", "30", "Consumer Staples", "35", "Health Care",
    "40", "Financials", "45", "Information Technology", "50", "Communication Services",
    "55", "Utilities", "60", "Real Estate"
  )
}

add_gics_names <- function(data) {
  gics_map <- gics_mapping()
  if ("gic_sectors" %in% colnames(data)) {
    data <- data %>% left_join(gics_map, by = "gic_sectors")
  }
  return(data)
}

# Format numeric columns efficiently using purrr
format_numeric_cols <- function(data, rounding_map) {
  data %>%
    mutate(across(
      any_of(names(rounding_map)),
      ~ if (is.numeric(.x)) {
        digits <- rounding_map[[cur_column()]]
        if (is.null(digits)) .x else round(.x, digits)
      } else .x
    ))
}

# ============================================================================
# DATA LOADING AND MERGE
# ============================================================================

workspace_file <- "notes/merged_data_workspace.RData"

if (file.exists(workspace_file)) {
  load(workspace_file)
  if (!exists("con") || !DBI::dbIsValid(con)) {
    con <- connect_wharton()
  }
} else {
  con <- connect_wharton()
  cusip_target <- NULL
  start_year <- 1980
  end_year <- 2024
  start_date <- as.Date("1980-01-01")
  end_date <- as.Date("2024-12-31")
  begfyr_expr <- dbplyr::sql("datadate - interval '11 months'")

  if (!is.null(cusip_target)) {
    cusip_gvkey <- tbl(con, in_schema("comp", "names")) %>%
      filter(substr(cusip, 1, 8) == !!cusip_target) %>%
      distinct(gvkey)
  } else {
    cusip_gvkey <- NULL
  }

  ccm_link <- tbl(con, in_schema("crsp", "ccmxpf_lnkhist")) %>%
    filter(linktype %in% c("LU", "LC"), linkprim %in% c("P", "C")) %>%
    transmute(
      gvkey, permno = lpermno, permco = lpermco,
      linkdt, linkenddt = coalesce(linkenddt, as.Date("9999-12-31"))
    )

  if (!is.null(cusip_gvkey)) {
    ccm_link <- ccm_link %>% inner_join(cusip_gvkey, by = "gvkey")
  }

  funda <- tbl(con, in_schema("comp", "funda")) %>%
    filter(fyear >= !!start_year-10, fyear <= !!end_year) %>%
    filter(indfmt == "INDL", datafmt == "STD", popsrc == "D", consol == "C") %>%
    filter(curcd == "USD", fic == "USA", between(exchg, 11, 19)) %>%
    filter(is.na(sich) | (!between(sich, 6000, 6999) & sich != 2834)) %>%
    left_join(
      tbl(con, in_schema("comp", "names")) %>%
        select(gvkey, sic, naics, gsubind, gind),
      by = "gvkey"
    ) %>%
    mutate(gsector = substr(gind, 1, 2)) %>%
    { if (!is.null(cusip_gvkey)) inner_join(., cusip_gvkey, by = "gvkey") else . } %>%
    mutate(
      endfyr = datadate, begfyr = !!begfyr_expr,
      available_date = datadate + dbplyr::sql("interval '3 months'")
    ) %>%
    select(
      gvkey, datadate, fyear, fyr, apdedate, fdate, pdate,
      conm, tic, cusip, cik, ni, xrd, gsector,
      endfyr, begfyr, available_date
    )

  msf <- tbl(con, in_schema("crsp", "msf")) %>%
    select(permno, date, ret, prc, vol, shrout) %>%
    filter(date >= !!start_date, date <= !!end_date) %>%
    filter(!is.na(ret), !is.na(prc), !is.na(shrout)) %>%
    inner_join(
      tbl(con, in_schema("crsp", "msenames")) %>%
        select(permno, namedt, nameendt, siccd, shrcd, exchcd),
      by = "permno"
    ) %>%
    filter(
      date >= namedt, date <= coalesce(nameendt, as.Date("9999-12-31")),
      !between(siccd, 6000, 6999), siccd != 2834,
      shrcd %in% c(10, 11), exchcd %in% c(1, 2, 3)
    ) %>%
    mutate(mktcap = abs(prc * shrout)) %>%
    select(permno, date, ret, prc, vol, mktcap)

  delist <- tbl(con, in_schema("crsp", "msedelist")) %>%
    filter(dlstdt >= !!start_date, dlstdt <= !!end_date, !is.na(dlret)) %>%
    select(permno, date = dlstdt, dlret) %>%
    collect() %>%
    mutate(date = as.Date(date), dlret = as.numeric(dlret))

  link_msf <- msf %>%
    inner_join(ccm_link, by = "permno") %>%
    filter(date >= linkdt, date <= linkenddt) %>%
    group_by(permno, date) %>%
    slice_max(linkdt, n = 1, with_ties = FALSE) %>%
    ungroup()

  crsp_dates <- link_msf %>%
    distinct(gvkey, permno, date) %>%
    collect()

  merged <- crsp_dates %>%
    inner_join(
      funda %>% filter(available_date <= !!end_date) %>% collect(),
      by = "gvkey", relationship = "many-to-many"
    ) %>%
    filter(date >= available_date) %>%
    group_by(gvkey, permno, date) %>%
    slice_max(datadate, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    inner_join(
      link_msf %>%
        select(gvkey, permno, date, ret, prc, vol, mktcap) %>%
        rename(ret_crsp = ret, prc_crsp = prc) %>%
        collect() %>%
        mutate(prc_crsp = abs(as.numeric(prc_crsp)), ret_crsp = as.numeric(ret_crsp)),
      by = c("gvkey", "permno", "date")
    ) %>%
    left_join(delist, by = c("permno", "date")) %>%
    mutate(ret_eff = (1 + coalesce(ret_crsp, 0)) * (1 + coalesce(dlret, 0)) - 1) %>%
    select(-begfyr, -endfyr, -available_date, -dlret) %>%
    filter(!grepl("[A-Za-z]", ret_crsp), as.numeric(ret_crsp) >= -100) %>%
    {
      nm <- funda_name_mapping()
      rename_with(., ~ dplyr::coalesce(nm$snake_name[match(.x, nm$variable_name)], .x))
    } %>%
    add_gics_names()

  save(merged, con, start_year, end_year, start_date, end_date, cusip_target,
       file = workspace_file)
}

if (!exists("con") || !DBI::dbIsValid(con)) {
  con <- connect_wharton()
}
if (!exists("start_date")) {
  start_date <- as.Date("1980-01-01")
  end_date <- as.Date("2024-12-31")
  start_year <- 1980
  end_year <- 2024
  cusip_target <- NULL
}

# ============================================================================
# STATISTICAL ANALYSIS FUNCTIONS
# ============================================================================

calculate_portfolio_returns <- function(data, group_var = "research_not") {
  data %>%
    filter(!is.na(ret_crsp), !is.na(prc_crsp), !is.na(mktcap), ret_crsp != 0) %>%
    arrange(permno, date) %>%
    group_by(permno) %>%
    mutate(mktcap_lag = lag(mktcap), prc_lag = lag(prc_crsp)) %>%
    ungroup() %>%
    filter(!is.na(mktcap_lag), !is.na(prc_lag)) %>%
    mutate(date = ceiling_date(date, "month") - 1) %>%
    group_by(date, !!sym(group_var)) %>%
    summarize(
      ew_return = mean(ret_crsp, na.rm = TRUE),
      vw_return = sum((mktcap_lag / sum(mktcap_lag, na.rm = TRUE)) * ret_crsp, na.rm = TRUE),
      total_mktcap = sum(mktcap_lag, na.rm = TRUE),
      n_stocks = n(),
      .groups = "drop"
    ) %>%
    arrange(!!sym(group_var), date)
}

calculate_market_return <- function(con, start_date, end_date) {
  tbl(con, in_schema("crsp", "msi")) %>%
    filter(date >= !!start_date, date <= !!end_date) %>%
    select(date, vwretd, ewretd) %>%
    collect() %>%
    mutate(
      date = as.Date(date),
      vwretd = as.numeric(vwretd),
      ewretd = as.numeric(ewretd)
    ) %>%
    filter(!is.na(vwretd), !is.na(ewretd)) %>%
    arrange(date)
}

run_capm_regression <- function(portfolio_returns, market_returns, rf_rate = 0, use_ew_benchmark = FALSE) {
  market_col <- if (use_ew_benchmark) "ewretd" else "vwretd"
  reg_data <- portfolio_returns %>%
    left_join(
      market_returns %>%
        mutate(date = ceiling_date(date, "month") - 1) %>%
        select(date, market_return = !!sym(market_col)),
      by = "date"
    ) %>%
    mutate(
      portfolio_excess = portfolio_return - rf_rate,
      market_excess = market_return - rf_rate
    ) %>%
    filter(!is.na(portfolio_excess), !is.na(market_excess))

  if (nrow(reg_data) < 2) return(NULL)

  model <- lm(portfolio_excess ~ market_excess, data = reg_data)
  coef_summary <- summary(model)$coefficients
  conf_int <- confint(model, level = 0.95)

  n_months <- nrow(reg_data)
  arithmetic_monthly_return <- mean(reg_data$portfolio_return, na.rm = TRUE)
  monthly_vol <- sd(reg_data$portfolio_excess, na.rm = TRUE)

  tibble(
    arithmetic_monthly_return = arithmetic_monthly_return,
    annualized_return = (1 + arithmetic_monthly_return)^12 - 1,
    alpha = coef_summary[1, 1],
    beta = coef_summary[2, 1],
    alpha_tstat = coef_summary[1, 3],
    beta_tstat = coef_summary[2, 3],
    alpha_pval = coef_summary[1, 4],
    beta_pval = coef_summary[2, 4],
    alpha_lower_ci = conf_int[1, 1],
    alpha_upper_ci = conf_int[1, 2],
    beta_lower_ci = conf_int[2, 1],
    beta_upper_ci = conf_int[2, 2],
    r_squared = summary(model)$r.squared,
    sharpe_ratio = (arithmetic_monthly_return - rf_rate) / monthly_vol * sqrt(12),
    annualized_alpha = coef_summary[1, 1] * 12,
    annualized_vol = monthly_vol * sqrt(12),
    n_observations = n_months
  )
}

calculate_all_portfolio_metrics <- function(portfolio_data, market_data, portfolio_name, rf_rate = 0) {
  group_col <- names(portfolio_data)[2]
  group_sym <- sym(group_col)

  ew_returns <- portfolio_data %>% select(date, portfolio_return = ew_return, !!group_sym)
  vw_returns <- portfolio_data %>% select(date, portfolio_return = vw_return, !!group_sym)

  ew_metrics <- ew_returns %>%
    group_by(!!group_sym) %>%
    group_modify(~ {
      run_capm_regression(.x %>% select(date, portfolio_return), market_data, rf_rate, use_ew_benchmark = TRUE) %>%
        mutate(weighting = "Equal-Weighted")
    }) %>%
    ungroup()

  vw_metrics <- vw_returns %>%
    group_by(!!group_sym) %>%
    group_modify(~ {
      run_capm_regression(.x %>% select(date, portfolio_return), market_data, rf_rate, use_ew_benchmark = FALSE) %>%
        mutate(weighting = "Value-Weighted")
    }) %>%
    ungroup()

  bind_rows(ew_metrics, vw_metrics) %>%
    mutate(portfolio = paste(weighting, !!group_sym, sep = "_")) %>%
    select(portfolio, weighting, everything(), -!!group_sym)
}

calculate_sharpe_ratio <- function(returns, rf_rate = 0) {
  if (length(returns) == 0 || all(is.na(returns))) return(NA)
  excess_returns <- returns - rf_rate
  mean_excess <- mean(excess_returns, na.rm = TRUE)
  sd_returns <- sd(returns, na.rm = TRUE)
  if (sd_returns == 0 || is.na(sd_returns)) return(NA)
  (mean_excess / sd_returns) * sqrt(12)
}

# ============================================================================
# DATA PREPARATION
# ============================================================================

analysis_data <- merged %>%
  select(
    gvkey, permno, date, report_date, company_name, ticker,
    sector = sector_name, ret_crsp, mktcap,
    net_income_loss, research_and_development_expense, prc_crsp
  ) %>%
  mutate(research_not = if_else(is.na(research_and_development_expense), "Without_XRD", "With_XRD"))

market_returns <- calculate_market_return(con, start_date, end_date)
portfolio_returns <- calculate_portfolio_returns(analysis_data, "research_not")
portfolio_returns_sector <- analysis_data %>%
  filter(!is.na(sector)) %>%
  calculate_portfolio_returns("sector")

rf_rate_annual <- 0.03
rf_rate_monthly <- rf_rate_annual / 12

portfolio_metrics <- calculate_all_portfolio_metrics(
  portfolio_returns, market_returns, "R&D Portfolio", rf_rate_monthly
)

sector_metrics <- portfolio_returns_sector %>%
  group_by(sector) %>%
  group_modify(~ {
    run_capm_regression(.x %>% select(date, portfolio_return = vw_return), market_returns, rf_rate_monthly, use_ew_benchmark = FALSE)
  }) %>%
  ungroup() %>%
  arrange(desc(annualized_return))

# ============================================================================
# TABLE FORMATTING FUNCTIONS
# ============================================================================

format_performance_table <- function(metrics) {
  rounding_map <- list(
    arithmetic_monthly_return = 6, annualized_return = 4, alpha = 6, beta = 6,
    alpha_tstat = 6, beta_tstat = 2, alpha_lower_ci = 6, alpha_upper_ci = 6,
    beta_lower_ci = 6, beta_upper_ci = 6, r_squared = 6, sharpe_ratio = 6
  )

  metrics %>%
    mutate(Portfolio = portfolio) %>%
    format_numeric_cols(rounding_map) %>%
    mutate(
      `Alpha p-value` = formatC(alpha_pval, format = "e", digits = 3),
      `Beta p-value` = formatC(beta_pval, format = "e", digits = 3)
    ) %>%
    select(
      Portfolio, `Arithmetic Monthly Return` = arithmetic_monthly_return,
      `Annualized Return` = annualized_return, `Alpha (Monthly)` = alpha,
      Beta = beta, `Alpha t-stat` = alpha_tstat, `Beta t-stat` = beta_tstat,
      `Alpha p-value`, `Beta p-value`, `Alpha Lower CI (95%)` = alpha_lower_ci,
      `Alpha Upper CI (95%)` = alpha_upper_ci, `Beta Lower CI (95%)` = beta_lower_ci,
      `Beta Upper CI (95%)` = beta_upper_ci, `R-squared` = r_squared,
      `Sharpe Ratio` = sharpe_ratio
    )
}

format_sector_table <- function(metrics) {
  rounding_map <- list(
    annualized_return = 4, annualized_alpha = 4, beta = 6,
    sharpe_ratio = 4, r_squared = 4
  )

  metrics %>%
    mutate(Sector = sector) %>%
    format_numeric_cols(rounding_map) %>%
    select(
      Sector, `Annualized Return` = annualized_return,
      `Alpha (Annualized)` = annualized_alpha, Beta = beta,
      `Sharpe Ratio` = sharpe_ratio, `R-squared` = r_squared,
      `N Observations` = n_observations
    )
}

# ============================================================================
# CHART FUNCTIONS
# ============================================================================

create_performance_chart <- function(chart_data, panel_type = c("vw", "ew"),
                                      recessions, colors, start_date) {
  panel_type <- match.arg(panel_type)
  pattern <- if (panel_type == "vw") "vw_return" else "ew_return"
  benchmark_name <- if (panel_type == "vw") "CRSP VW Benchmark" else "CRSP EW Benchmark"
  panel_label <- if (panel_type == "vw") "Panel A: Value-Weighted Portfolios" else "Panel B: Equal-Weighted Portfolios"

  panel_data <- chart_data %>%
    filter(grepl(pattern, key)) %>%
    mutate(
      portfolio_name = case_when(
        grepl("With_XRD", key) ~ "R&D Portfolio",
        grepl("Without_XRD", key) ~ "No R&D Portfolio",
        grepl("CRSP", key) ~ benchmark_name,
        TRUE ~ key
      )
    ) %>%
    select(date, portfolio_name, cumvalue)

  chart <- hchart(panel_data, "line", hcaes(date, cumvalue, group = portfolio_name)) %>%
    hc_title(text = "Cumulative Performance Comparison (1980-2022)",
             style = list(fontSize = "25px", fontWeight = "bold")) %>%
    hc_subtitle(text = paste0(panel_label, "<br>Growth of $1 Invested in 1980"),
                style = list(fontSize = "20px")) %>%
    hc_colors(c(colors[["R&D Portfolio"]], colors[["No R&D Portfolio"]], colors[[benchmark_name]])) %>%
    hc_yAxis(title = list(text = "Cumulative Value ($)"), min = 0,
             labels = list(format = "${value:.2f}", style = list(fontSize = "16px"))) %>%
    hc_xAxis(title = list(text = "Date"), labels = list(style = list(fontSize = "16px"))) %>%
    hc_legend(align = "right", layout = "proximate", itemStyle = list(fontSize = "18px")) %>%
    hc_tooltip(pointFormat = "<b>{series.name}</b><br>Date: {point.x:%Y-%m}<br>Value: ${point.y:.2f}", shared = TRUE) %>%
    hc_plotOptions(line = list(lineWidth = 5), series = list(marker = list(enabled = FALSE))) %>%
    hc_add_theme(highcharter::hc_theme_darkunica())

  if (nrow(recessions) > 0) {
    plot_bands <- map(seq_len(nrow(recessions)), ~ list(
      from = as.numeric(recessions$start[.x]) * 86400000,
      to = as.numeric(recessions$end[.x]) * 86400000,
      color = "rgba(128, 128, 128, 0.2)",
      label = list(text = "", style = list(color = "gray"))
    ))
    chart <- chart %>% hc_xAxis(plotBands = plot_bands)
  }

  chart
}

# ============================================================================
# ALPHA TABLE GENERATION
# ============================================================================

create_alpha_table <- function(portfolio_metrics, market_returns, rf_rate_monthly) {
  benchmark_sharpe_vw <- calculate_sharpe_ratio(market_returns$vwretd, rf_rate_monthly)
  benchmark_sharpe_ew <- calculate_sharpe_ratio(market_returns$ewretd, rf_rate_monthly)

  benchmark_rows <- tibble(
    Portfolio = c("CRSP VW Benchmark", "CRSP EW Benchmark"),
    `Alpha (monthly %)` = c("—", "—"), `t-statistic` = c("—", "—"),
    `p-value` = c("—", "—"), Beta = c("—", "—"), `R²` = c("—", "—"),
    Sharpe = c(round(benchmark_sharpe_vw, 2), round(benchmark_sharpe_ew, 2)),
    is_significant = c(FALSE, FALSE), sort_order = c(5, 6)
  )

  alpha_table <- portfolio_metrics %>%
    mutate(
      Portfolio = case_when(
        grepl("Equal-Weighted.*With_XRD", portfolio) ~ "R&D EW",
        grepl("Equal-Weighted.*Without_XRD", portfolio) ~ "No R&D EW",
        grepl("Value-Weighted.*With_XRD", portfolio) ~ "R&D VW",
        grepl("Value-Weighted.*Without_XRD", portfolio) ~ "No R&D VW",
        TRUE ~ portfolio
      ),
      alpha_monthly_pct = round(alpha * 100, 2),
      alpha_star = case_when(
        alpha_pval < 0.01 ~ "***", alpha_pval < 0.05 ~ "**",
        alpha_pval < 0.10 ~ "*", TRUE ~ ""
      ),
      `Alpha (monthly %)` = paste0(sprintf("%.2f", alpha_monthly_pct), alpha_star),
      `t-statistic` = round(alpha_tstat, 2),
      `p-value` = formatC(alpha_pval, format = "f", digits = 3),
      Beta = round(beta, 2), `R²` = round(r_squared, 2),
      Sharpe = round(sharpe_ratio, 2),
      is_significant = alpha_pval < 0.10,
      sort_order = case_when(
        Portfolio == "R&D VW" ~ 1, Portfolio == "No R&D VW" ~ 2,
        Portfolio == "R&D EW" ~ 3, Portfolio == "No R&D EW" ~ 4, TRUE ~ 5
      )
    ) %>%
    arrange(sort_order) %>%
    select(Portfolio, `Alpha (monthly %)`, `t-statistic`, `p-value`, Beta, `R²`, Sharpe, is_significant, sort_order) %>%
    mutate_all(as.character) %>%
    bind_rows(benchmark_rows %>% mutate_all(as.character)) %>%
    arrange(sort_order) %>%
    select(-sort_order)

  alpha_table
}

create_alpha_table_html <- function(alpha_table, custom_css) {
  alpha_table_for_display <- alpha_table %>% select(-is_significant)

  html_table <- alpha_table_for_display %>%
    kable(format = "html",
          caption = "<b>Alpha Estimation Results: Single-Factor Model</b><br><i>Rp - Rƒ = α + β(R_CRSP_VW - Rƒ)</i>",
          align = c("l", "r", "r", "r", "r", "r", "r"), escape = FALSE) %>%
    kable_styling(bootstrap_options = c("striped", "hover"), full_width = FALSE,
                  position = "left", font_size = "1rem",
                  htmltable_class = "table table-dark table-striped") %>%
    row_spec(which(alpha_table_for_display$Portfolio %in% c("R&D VW", "R&D EW")),
             color = "#dd5182", bold = TRUE) %>%
    row_spec(which(alpha_table_for_display$Portfolio %in% c("No R&D VW", "No R&D EW")),
             color = "#ffa600", bold = TRUE) %>%
    row_spec(which(alpha_table_for_display$Portfolio %in% c("CRSP VW Benchmark", "CRSP EW Benchmark")),
             color = "white", bold = TRUE) %>%
    column_spec(2, bold = c(alpha_table$is_significant, rep(FALSE, 2))) %>%
    footnote(general_title = "", footnote_as_chunk = TRUE, escape = FALSE, threeparttable = TRUE)

  htmltools::HTML(paste0("<!DOCTYPE html><html><head>", custom_css, "</head><body>",
                         as.character(html_table), "</body></html>"))
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

performance_table <- format_performance_table(portfolio_metrics)
sector_table <- format_sector_table(sector_metrics)

# Prepare chart data
chart_data <- portfolio_returns %>%
  tidyr::gather(key, value, ew_return:vw_return) %>%
  tidyr::unite(key, key, research_not, sep = "_") %>%
  bind_rows(
    market_returns %>%
      mutate(date = ceiling_date(date, "month") - 1) %>%
      mutate(key = "vw_return_CRSP_VW_Benchmark", value = vwretd) %>%
      select(date, key, value)
  ) %>%
  bind_rows(
    market_returns %>%
      mutate(date = ceiling_date(date, "month") - 1) %>%
      mutate(key = "ew_return_CRSP_EW_Benchmark", value = ewretd) %>%
      select(date, key, value)
  ) %>%
  filter(date >= start_date) %>%
  arrange(key, date) %>%
  group_by(key) %>%
  mutate(cumvalue = cumprod(1 + value)) %>%
  ungroup()

recessions <- tibble(
  start = as.Date(c("1980-01-01", "1981-07-01", "1990-07-01", "2001-03-01", "2007-12-01", "2020-02-01")),
  end = as.Date(c("1980-07-01", "1982-11-01", "1991-03-01", "2001-11-01", "2009-06-01", "2020-04-01"))
) %>% filter(start >= start_date, start <= end_date)

colors <- list(
  "R&D Portfolio" = "#ff6361", "No R&D Portfolio" = "#bc5090",
  "CRSP VW Benchmark" = "#58508d", "CRSP EW Benchmark" = "#58508d"
)

chart_a <- create_performance_chart(chart_data, "vw", recessions, colors, start_date)
chart_b <- create_performance_chart(chart_data, "ew", recessions, colors, start_date)

custom_css <- "
<style>
@import url('https://fonts.googleapis.com/css2?family=Tomorrow:wght@400;600;700&display=swap');
body { font-family: 'Tomorrow', sans-serif; background-color: #1d1d1c; padding: 1rem; margin: 0; color: white; }
:root { font-size: 25px; }
.table-dark.table-striped tbody tr:nth-of-type(odd) { background-color: rgba(255, 255, 255, 0.05) !important; }
.table-dark.table-striped tbody tr:nth-of-type(even) { background-color: rgba(0, 0, 0, 0.15) !important; }
.table-dark tbody tr[style*='color'] { background-color: inherit !important; }
.table-dark td, .table-dark th { padding: 0.5rem 0.75rem !important; }
</style>
"

alpha_table <- create_alpha_table(portfolio_metrics, market_returns, rf_rate_monthly)
alpha_table_html_wrapped <- create_alpha_table_html(alpha_table, custom_css)

# Display outputs
if (interactive()) {
  if (requireNamespace("zgtools", quietly = TRUE)) {
    library(zgtools)
    performance_table %>% kable(format = "html", digits = 6) %>%
      kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"),
                    full_width = FALSE, position = "left") %>%
      htmltools::browsable()

    sector_table %>% kable(format = "html", digits = 4) %>%
      kable_styling(bootstrap_options = c("striped", "hover", "condensed", "responsive"),
                    full_width = FALSE, position = "left") %>%
      htmltools::browsable()

    chart_a
    chart_b
    alpha_table_html_wrapped %>% htmltools::browsable()
  }
}

# Save workspace
if (exists("merged")) {
  save(merged, portfolio_returns, portfolio_returns_sector, market_returns,
       portfolio_metrics, sector_metrics, performance_table, sector_table,
       chart_a, chart_b, analysis_data, alpha_table, alpha_table_html_wrapped,
       con, start_year, end_year, start_date, end_date, cusip_target,
       file = workspace_file)
}

