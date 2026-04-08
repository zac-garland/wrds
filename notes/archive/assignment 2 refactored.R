library(tidyverse)

get_tick_returns <- function(tickers, start_date = as_date("1990-01-01"), bypass_date_req = FALSE) {
  out_ret <- tidyquant::tq_get(tickers, from = start_date) %>%
    select(symbol, date, adjusted) %>%
    group_by(symbol, month_year = paste(month(date), year(date))) %>%
    filter(date == max(date, na.rm = TRUE)) %>%
    group_by(symbol) %>%
    mutate(date = floor_date(date, "month")) %>%
    mutate(ret = adjusted / lag(adjusted) - 1) %>%
    ungroup() %>%
    select(symbol, date, ret) %>%
    filter(!is.na(ret)) %>%
    spread(symbol, ret)

  check_na_returns <- out_ret %>%
    select(-date) %>%
    summarize_all(list(~sum(is.na(.)))) %>%
    gather(key, value) %>%
    filter(value != 0)

  if(nrow(check_na_returns) > 0 & bypass_date_req == FALSE){
    stop(glue::glue("{pull(check_na_returns,key) %>% paste(collapse = ',')} stocks have missing returns. replace with new stocks or date periods"))
  }else{
    na.omit(out_ret)
  }
}

calc_lw_cov <- function(ret_mat) {
  beta_calc <- function(row_vals) {
    outer_prod <- outer(row_vals, row_vals)
    sum((outer_prod - cov_mat)^2)
  }
  cov_mat <- cov(ret_mat)
  emat <- as.matrix(ret_mat %>% mutate_all(list(~ . - mean(., na.rm = TRUE))))
  n <- nrow(ret_mat)
  p <- ncol(ret_mat)
  avg_var <- mean(diag(cov_mat))
  F_mat <- diag(p) * avg_var
  beta_sum <- emat %>%
    as_tibble() %>%
    mutate(beta = pmap_dbl(., ~ beta_calc(c(...)))) %>%
    pull(beta) %>%
    sum(na.rm = TRUE)
  beta_sq <- beta_sum / (n^2)
  delta_sq <- sum((cov_mat - F_mat)^2, na.rm = TRUE)
  lambda <- min(1, beta_sq / delta_sq)
  lambda * F_mat + (1 - lambda) * cov_mat
}

calc_constrained_weights <- function(lwcovmat, mu_vec = NULL, portfolio_type = "gmv", rf = 0.035 / 12) {
  library(CVXR)
  p <- ncol(lwcovmat)

  # Define separate variables for long and short positions
  w_long <- Variable(p, nonneg = TRUE)   # long positions (≥ 0)
  w_short <- Variable(p, nonneg = TRUE)  # short positions as positive values (≥ 0)
  w <- w_long - w_short                  # net position = long - short

  # 130/30 constraints
  constraints <- list(
    sum(w_long) == 1.3,    # 130% long exposure
    sum(w_short) == 0.3    # 30% short exposure
  )
  # Note: sum(w) = sum(w_long) - sum(w_short) = 1.3 - 0.3 = 1.0 (fully invested)

  if (portfolio_type == "gmv") {
    objective <- Minimize(quad_form(w, lwcovmat))

  } else if (portfolio_type == "orp") {
    if (is.null(mu_vec)) {
      stop("Need expected returns for ORP")
    }
    excess_ret <- as.numeric(mu_vec) - rf
    # Maximize expected return minus risk penalty
    objective <- Maximize(t(excess_ret) %*% w - 0.001 * quad_form(w, lwcovmat))
  }

  prob <- Problem(objective, constraints)
  result <- solve(prob)

  if (result$status != "optimal") {
    warning(paste("Optimization status:", result$status))
  }

  as.vector(result$getValue(w))
}

rolling_backtest_returns <- function(input_data, window_months = 60, rf = 0.035 / 12) {
  ret_data <- input_data %>% arrange(date)
  dates <- ret_data$date
  ret_mat <- ret_data %>% select(-date)
  n_obs <- nrow(ret_data)
  rebal_dates <- seq(window_months + 1, n_obs, by = 1)

  map_dfr(rebal_dates, function(end_idx) {
    start_idx <- max(1, end_idx - window_months)
    train_data <- ret_mat[start_idx:end_idx, ]

    mu_vec <- train_data %>% summarize_all(list(~ mean(., na.rm = TRUE)))
    lwcovmat <- calc_lw_cov(train_data)

    gmv_weights <- calc_constrained_weights(lwcovmat, portfolio_type = "gmv")
    orp_weights <- calc_constrained_weights(lwcovmat, mu_vec = mu_vec, portfolio_type = "orp", rf = rf)

    if (end_idx < n_obs) {
      oos_returns <- as.numeric(ret_mat[end_idx + 1, ])
      gmv_return <- sum(gmv_weights * oos_returns)
      orp_return <- sum(orp_weights * oos_returns)
    } else {
      gmv_return <- NA
      orp_return <- NA
    }

    tibble(
      date = dates[end_idx],
      gmv_return = gmv_return,
      orp_return = orp_return
    )
  })
}

rolling_backtest_weights <- function(input_data, window_months = 60, rf = 0.035 / 12) {
  ret_data <- input_data %>% arrange(date)
  dates <- ret_data$date
  ret_mat <- ret_data %>% select(-date)
  asset_names <- names(ret_mat)
  n_obs <- nrow(ret_data)
  rebal_dates <- seq(window_months + 1, n_obs, by = 1)

  map_dfr(rebal_dates, function(end_idx) {
    start_idx <- max(1, end_idx - window_months)
    train_data <- ret_mat[start_idx:end_idx, ]

    mu_vec <- train_data %>% summarize_all(list(~ mean(., na.rm = TRUE)))
    lwcovmat <- calc_lw_cov(train_data)

    gmv_weights <- calc_constrained_weights(lwcovmat, portfolio_type = "gmv")
    orp_weights <- calc_constrained_weights(lwcovmat, mu_vec = mu_vec, portfolio_type = "orp", rf = rf)

    bind_rows(
      tibble(date = dates[end_idx], portfolio = "gmv", asset = asset_names, weight = gmv_weights),
      tibble(date = dates[end_idx], portfolio = "orp", asset = asset_names, weight = orp_weights)
    )
  })
}

calc_performance_metrics <- function(returns_df) {
  perf_data <- returns_df %>%
    filter(!is.na(gmv_return)) %>%
    mutate(
      gmv_cumret = cumprod(1 + gmv_return),
      orp_cumret = cumprod(1 + orp_return)
    )

  bind_rows(
    tibble(
      portfolio = "gmv",
      total_return = tail(perf_data$gmv_cumret, 1) - 1,
      annualized_return = (tail(perf_data$gmv_cumret, 1)^(12 / nrow(perf_data))) - 1,
      volatility = sd(perf_data$gmv_return) * sqrt(12),
      sharpe = mean(perf_data$gmv_return) * sqrt(12) / (sd(perf_data$gmv_return) * sqrt(12))
    ),
    tibble(
      portfolio = "orp",
      total_return = tail(perf_data$orp_cumret, 1) - 1,
      annualized_return = (tail(perf_data$orp_cumret, 1)^(12 / nrow(perf_data))) - 1,
      volatility = sd(perf_data$orp_return) * sqrt(12),
      sharpe = mean(perf_data$orp_return) * sqrt(12) / (sd(perf_data$orp_return) * sqrt(12))
    )
  )
}

calc_cumulative_returns <- function(returns_df) {
  returns_df %>%
    filter(!is.na(gmv_return)) %>%
    mutate(
      gmv_cumret = cumprod(1 + gmv_return),
      orp_cumret = cumprod(1 + orp_return)
    )
}

calc_turnover <- function(weights_df) {
  weights_df %>%
    arrange(portfolio, asset, date) %>%
    group_by(portfolio, asset) %>%
    mutate(weight_change = abs(weight - lag(weight, default = 0))) %>%
    group_by(portfolio, date) %>%
    summarise(turnover = sum(weight_change, na.rm = TRUE), .groups = "drop")
}

calc_weight_stability <- function(weights_df) {
  weights_df %>%
    group_by(portfolio, date) %>%
    summarise(weight_volatility = sd(weight), .groups = "drop")
}

calc_exposure_analysis <- function(weights_df) {
  weights_df %>%
    mutate(
      long_weight = pmax(weight, 0),
      short_weight = pmax(-weight, 0)
    ) %>%
    group_by(portfolio, date) %>%
    summarise(
      total_long = sum(long_weight),
      total_short = sum(short_weight),
      net_exposure = sum(weight),
      gross_exposure = sum(abs(weight)),
      .groups = "drop"
    )
}

# Portfolio definition
portfolio_tickers <- c(
  "MSFT", "AAPL", "IBM", "ORCL", "JPM", "BAC", "WFC", "AXP",
  "JNJ", "PFE", "MRK", "UNH", "HD", "MCD", "DIS", "NKE",
  "PG", "KO", "PEP", "WMT", "XOM", "CVX", "COP", "GE",
  "CAT", "BA", "DD", "NEE", "GLD", "USO"
)

# Execute backtest
input_df <- get_tick_returns(portfolio_tickers, bypass_date_req = TRUE)
backtest_returns <- rolling_backtest_returns(input_df, window_months = 60)
backtest_weights <- rolling_backtest_weights(input_df, window_months = 60)

# Analysis
performance_metrics <- calc_performance_metrics(backtest_returns)
cumulative_returns <- calc_cumulative_returns(backtest_returns)
turnover_data <- calc_turnover(backtest_weights)
stability_data <- calc_weight_stability(backtest_weights)
exposure_analysis <- calc_exposure_analysis(backtest_weights)

# View results
print("Performance Metrics:")
print(performance_metrics)

print("Exposure Analysis (first 10 rows):")
print(head(exposure_analysis, 10))
