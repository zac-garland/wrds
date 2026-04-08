# Load required libraries
library(tidyverse)
library(dbplyr)
library(RPostgres)
library(lubridate)
library(ggplot2)
library(corrplot)
library(PerformanceAnalytics)

# Source the wharton connection functions
# source("../R/wharton-connections.R")
devtools::load_all()

# Parameters
START_DATE <- "1980-01-01"
END_DATE <- "2022-12-31"
TOP_N <- 100

cat("Period:", START_DATE, "to", END_DATE, "| Top", TOP_N, "stocks\n")

# =============================================================================
# DATA RETRIEVAL AND CLEANING
# =============================================================================

# Connect to WRDS
con <- connect_wharton()
cat("Connected to WRDS\n")

fundamentals_table <- wharton_table("comp","g_funda")$result
ccm_tablex <- wharton_table('crsp',"crsp_a_ccm")

fundamentals_table %>% filter(!between(sich,6000,6999),sich != 2834)

wmt_preview <- tbl(con,in_schema('crsp','msenames')) %>% filter(comnam %like% "%WALMART%")

wmt_preview %>% glimpse()


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

# Load the data
cat("[1/5] Pulling CRSP panel...\n")
panel <- get_crsp_monthly_panel(con, START_DATE, END_DATE)
cat("Rows:", nrow(panel), "\n")

cat("[2/5] Adding delisting returns...\n")
delist <- get_delistings(con, START_DATE, END_DATE)
panel <- add_effective_returns(panel, delist)

cat("Final:", nrow(panel), "rows | Securities:", n_distinct(panel$permno), "\n")
cat("Period:", min(panel$date), "to", max(panel$date), "\n")

# =============================================================================
# INDEX CONSTRUCTION
# =============================================================================

# Function to build top-N indexes
build_topn_indexes <- function(panel, top_n) {
  # Sort data
  panel <- panel %>%
    arrange(date, permno)

  # Get unique months
  months <- sort(unique(panel$date))

  # Initialize index levels
  ew_level <- c(100.0)
  vw_level <- c(100.0)
  pw_level <- c(100.0)

  # Process each month
  for (i in 2:length(months)) {
    t_1 <- months[i - 1]
    t <- months[i]

    # Get data for t-1 (selection) and t (returns)
    g_t1 <- panel %>%
      filter(date == t_1) %>%
      filter(!is.na(mktcap), !is.na(prc))

    g_t <- panel %>%
      filter(date == t)

    if (nrow(g_t1) == 0 || nrow(g_t) == 0) {
      ew_level <- c(ew_level, ew_level[length(ew_level)])
      vw_level <- c(vw_level, vw_level[length(vw_level)])
      pw_level <- c(pw_level, pw_level[length(pw_level)])
      next
    }

    # Select top-N by market cap at t-1
    chosen <- g_t1 %>%
      arrange(desc(mktcap)) %>%
      head(top_n) %>%
      select(permno, mktcap, prc)

    # Get returns for selected stocks at time t
    common_stocks <- g_t %>%
      filter(permno %in% chosen$permno) %>%
      select(permno, ret_eff)

    if (nrow(common_stocks) == 0) {
      ew_level <- c(ew_level, ew_level[length(ew_level)])
      vw_level <- c(vw_level, vw_level[length(vw_level)])
      pw_level <- c(pw_level, pw_level[length(pw_level)])
      next
    }

    # Merge with selection data
    returns_data <- common_stocks %>%
      left_join(chosen, by = "permno") %>%
      filter(!is.na(ret_eff))

    if (nrow(returns_data) == 0) {
      ew_level <- c(ew_level, ew_level[length(ew_level)])
      vw_level <- c(vw_level, vw_level[length(vw_level)])
      pw_level <- c(pw_level, pw_level[length(pw_level)])
      next
    }

    # Equal-Weighted return
    ew_gross <- mean(1 + returns_data$ret_eff, na.rm = TRUE)
    if (is.na(ew_gross)) ew_gross <- 1.0

    # Value-Weighted return
    valid_vw <- returns_data %>%
      filter(!is.na(mktcap), !is.na(ret_eff))

    if (nrow(valid_vw) > 0 && sum(valid_vw$mktcap) > 0) {
      vw_weights <- valid_vw$mktcap / sum(valid_vw$mktcap)
      vw_gross <- sum((1 + valid_vw$ret_eff) * vw_weights)
    } else {
      vw_gross <- 1.0
    }

    # Price-Weighted return
    valid_pw <- returns_data %>%
      filter(!is.na(prc), !is.na(ret_eff))

    if (nrow(valid_pw) > 0 && sum(valid_pw$prc) > 0) {
      pw_weights <- valid_pw$prc / sum(valid_pw$prc)
      pw_gross <- sum((1 + valid_pw$ret_eff) * pw_weights)
    } else {
      pw_gross <- 1.0
    }

    # Update index levels
    ew_level <- c(ew_level, ew_level[length(ew_level)] * ew_gross)
    vw_level <- c(vw_level, vw_level[length(vw_level)] * vw_gross)
    pw_level <- c(pw_level, pw_level[length(pw_level)] * pw_gross)
  }

  # Create results dataframe
  result <- data.frame(
    date = months[2:length(months)],
    `Equal-Weighted` = ew_level[2:length(ew_level)],
    `Value-Weighted` = vw_level[2:length(vw_level)],
    `Price-Weighted` = pw_level[2:length(pw_level)],
    check.names = FALSE
  )

  return(result)
}

cat("[3/5] Building custom indices...\n")
custom_levels <- build_topn_indexes(panel, TOP_N)
cat("Indices built:", nrow(custom_levels), "observations\n")

# =============================================================================
# ETF BENCHMARKS
# =============================================================================

# Function to find PERMNO for ticker around specific date
pick_permno_for_ticker <- function(con, ticker, around_date) {
  result <- tbl(con, in_schema("crsp", "msenames")) %>%
    filter(
      ticker == !!ticker,
      namedt <= !!around_date,
      nameendt >= !!around_date | is.na(nameendt)
    ) %>%
    arrange(desc(namedt)) %>%
    head(1) %>%
    select(permno) %>%
    collect()

  if (nrow(result) > 0) {
    return(result$permno[1])
  } else {
    return(NULL)
  }
}

# Function to get ETF index
get_etf_index <- function(con, permno, start_date, end_date, label) {
  etf_data <- tbl(con, in_schema("crsp", "msf")) %>%
    filter(
      permno == !!permno,
      date >= !!start_date,
      date <= !!end_date
    ) %>%
    select(date, ret) %>%
    arrange(date) %>%
    collect() %>%
    mutate(
      date = as.Date(date),
      ret = as.numeric(ret),
      ret = ifelse(is.na(ret), 0, ret)
    )

  # Calculate index level
  etf_data <- etf_data %>%
    mutate(index_level = cumprod(1 + ret) * 100)

  # Return as named vector
  index_series <- etf_data$index_level
  names(index_series) <- as.character(etf_data$date)

  return(index_series)
}

cat("[4/5] Getting ETF benchmarks...\n")
etf_tickers <- c("SPY", "IWM", "QQQ")
etf_series <- list()

for (tkr in etf_tickers) {
  pno <- pick_permno_for_ticker(con, tkr, as.Date(START_DATE))
  if (!is.null(pno)) {
    etf_series[[tkr]] <- get_etf_index(con, pno, START_DATE, END_DATE, tkr)
    cat("Retrieved", tkr, ":", length(etf_series[[tkr]]), "observations\n")
  }
}

# =============================================================================
# ANALYSIS AND VISUALIZATION
# =============================================================================

# Function to align series and compute correlations
align_and_corr <- function(levels_list) {
  # Find common dates
  common_dates <- Reduce(intersect, lapply(levels_list, function(x) names(x)))
  common_dates <- sort(as.Date(common_dates))

  # Align all series
  aligned_data <- lapply(levels_list, function(x) x[as.character(common_dates)])

  # Create matrix
  panel_matrix <- do.call(cbind, aligned_data)
  colnames(panel_matrix) <- names(levels_list)

  # Calculate log returns
  log_returns <- log(panel_matrix / lag(panel_matrix, 1))
  log_returns <- log_returns[-1, ] # Remove first row (NA)

  # Calculate correlation matrix
  corr_matrix <- cor(log_returns, use = "complete.obs")

  return(corr_matrix)
}

cat("[5/5] Computing correlations...\n")
# all_series <- c(
#   list(custom_levels$`Equal-Weighted`),
#   list(custom_levels$`Value-Weighted`),
#   list(custom_levels$`Price-Weighted`),
#   etf_series %>% lapply(function(x) x[-1])
# )
# names(all_series) <- c("Equal-Weighted", "Value-Weighted", "Price-Weighted", names(etf_series))
#
# # Convert to named vectors for alignment
# for (i in 1:length(all_series)) {
#   if (is.data.frame(all_series[[i]])) {
#     series_vec <- all_series[[i]][[2]]  # Get the values column
#     names(series_vec) <- as.character(all_series[[i]]$date)
#     all_series[[i]] <- series_vec
#   }
# }
#
# corr_mat <- align_and_corr(all_series)
# cat("Correlation matrix computed\n")


all_series <- custom_levels %>%
  as_tibble() %>%
  left_join(etf_series %>% enframe("ticker", "data") %>% spread(ticker, data) %>% unnest() %>% mutate(date = names(etf_series[[1]]) %>% as_date())) %>%
  gather(key, value, -date)

# =============================================================================
# PERFORMANCE VISUALIZATION
# =============================================================================

# # Create performance plots
# p1 <- ggplot() +
#   geom_line(data = custom_levels, aes(x = date, y = `Equal-Weighted`, color = "Custom Equal-Weighted"), size = 1) +
#   geom_line(data = custom_levels, aes(x = date, y = `Value-Weighted`, color = "Custom Value-Weighted"), size = 1) +
#   geom_line(data = custom_levels, aes(x = date, y = `Price-Weighted`, color = "Custom Price-Weighted"), size = 1) +
#   labs(
#     title = paste0("Index Performance (Top-", TOP_N, " vs ETFs)"),
#     y = "Index Level (Base=100)",
#     x = "Date"
#   ) +
#   theme_minimal() +
#   theme(legend.position = "bottom")
#
# # Add ETF lines if available
# if (length(etf_series) > 0) {
#   for (etf_name in names(etf_series)) {
#     etf_data <- data.frame(
#       date = as.Date(names(etf_series[[etf_name]])),
#       value = as.numeric(etf_series[[etf_name]])
#     )
#     p1 <- p1 + geom_line(data = etf_data, aes(x = date, y = value, color = etf_name), linetype = "dashed", alpha = 0.8)
#   }
# }
#
# print(p1)
#
# # Custom index comparison
# p2 <- ggplot(custom_levels, aes(x = date)) +
#   geom_line(aes(y = `Equal-Weighted`, color = "Equal-Weighted"), size = 1) +
#   geom_line(aes(y = `Value-Weighted`, color = "Value-Weighted"), size = 1) +
#   geom_line(aes(y = `Price-Weighted`, color = "Price-Weighted"), size = 1) +
#   labs(
#     title = "Custom Index Comparison",
#     y = "Index Level",
#     x = "Date"
#   ) +
#   theme_minimal() +
#   theme(legend.position = "bottom")
#
# print(p2)
#
# # Returns distribution
# returns <- custom_levels %>%
#   select(-date) %>%
#   mutate_all(~ (. / lag(.) - 1)) %>%
#   select(-1) # Remove first row with NAs
#
# p3 <- returns %>%
#   pivot_longer(everything(), names_to = "Index", values_to = "Return") %>%
#   ggplot(aes(x = Return, fill = Index)) +
#   geom_histogram(bins = 50, alpha = 0.7) +
#   facet_wrap(~Index, scales = "free") +
#   labs(
#     title = "Monthly Return Distributions",
#     x = "Monthly Return"
#   ) +
#   theme_minimal()
#
# print(p3)
#
# # Correlation heatmap
# p4 <- corr_mat %>%
#   as.data.frame() %>%
#   rownames_to_column("Index1") %>%
#   pivot_longer(-Index1, names_to = "Index2", values_to = "Correlation") %>%
#   ggplot(aes(x = Index2, y = Index1, fill = Correlation)) +
#   geom_tile() +
#   geom_text(aes(label = round(Correlation, 2)), color = "white", size = 3) +
#   scale_fill_gradient2(low = "blue", mid = "white", high = "red", midpoint = 0) +
#   labs(
#     title = "Correlation Matrix (Log Returns)",
#     x = "",
#     y = ""
#   ) +
#   theme_minimal() +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))
#
# print(p4)

# =============================================================================
# PERFORMANCE SUMMARY
# =============================================================================

# Calculate performance statistics
performance_stats <- custom_levels %>%
  select(-date) %>%
  mutate_all(~ (. / lag(.) - 1)) %>%
  select(-1) %>% # Remove first row with NAs
  summarise_all(list(
    `Annualized Return (%)` = ~ ((1 + mean(., na.rm = TRUE))^12 - 1) * 100,
    `Annualized Volatility (%)` = ~ sd(., na.rm = TRUE) * sqrt(12) * 100,
    `Sharpe Ratio` = ~ ((1 + mean(., na.rm = TRUE))^12 - 1) / (sd(., na.rm = TRUE) * sqrt(12)),
    `Max Drawdown (%)` = ~ min(cummax(custom_levels[[cur_column()]]) / custom_levels[[cur_column()]] - 1, na.rm = TRUE) * 100,
    `Final Level` = ~ last(custom_levels[[cur_column()]])
  )) %>%
  pivot_longer(everything(), names_to = "Metric", values_to = "Value") %>%
  separate(Metric, into = c("Index", "Metric"), sep = "\\.") %>%
  pivot_wider(names_from = Metric, values_from = Value)

cat("Performance Statistics:\n")
cat("=" %R% 50, "\n")
print(performance_stats)

# Find best performer
best_performer <- performance_stats %>%
  filter(Metric == "Annualized Return (%)") %>%
  slice_max(Value) %>%
  pull(Index)

total_return <- (last(custom_levels[[best_performer]]) / 100 - 1) * 100
cat("\nBest Performer:", best_performer, "\n")
cat("Total Return:", round(total_return, 1), "%\n")

cat("\nCorrelation Matrix:\n")
print(round(corr_mat, 3))

# Close connection
dbDisconnect(con)
cat("\nWRDS connection closed\n")

# =============================================================================
# KEY FINDINGS AND EXTENSIONS
# =============================================================================

cat("\nKey Findings:\n")
cat("- Successfully constructed three custom equity indices using top-", TOP_N, " stocks\n")
cat("- Incorporated delisting returns to mitigate survivorship bias\n")
cat("- Applied monthly reconstitution using lagged market cap information\n")
cat("- Compared performance against major ETF benchmarks\n")

cat("\nPotential Extensions:\n")
cat("- Alternative reconstitution frequencies (daily, quarterly)\n")
cat("- Transaction cost analysis and turnover metrics\n")
cat("- Factor exposure analysis and style drift examination\n")
cat("- Risk metrics (VaR, conditional VaR)\n")
cat("- Sector constraints and industry-neutral indices\n")

cat("\nBest Practices:\n")
cat("1. Data Validation: Verify constituent selection and return calculations\n")
cat("2. Bias Mitigation: Use lagged information, include delisting returns\n")
cat("3. Scalability: Time-windowed queries for large datasets\n")
cat("4. Documentation: Clear methodology and assumption documentation\n")
cat("5. Testing: Validate on small samples before full analysis\n")
