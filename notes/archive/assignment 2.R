library(tidyverse)

get_tick_returns <- function(tickers,start_date = as_date("1990-01-01"), bypass_date_req = FALSE) {
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


# 1. Existing functions (unchanged)
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

# 2. Constrained portfolio optimization
calc_constrained_weights <- function(lwcovmat, mu_vec = NULL,
                                     max_long = 0.8, max_short = .2,
                                     portfolio_type = "gmv", rf = 0.035 / 12) {
  library(CVXR)
  p <- ncol(lwcovmat)
  w <- Variable(p)

  # Base constraints
  constraints <- list(
    sum(w) == 1,
    w <= max_long,
    w >= -max_short
  )

  if (portfolio_type == "gmv") {
    # GMV: minimize variance only
    objective <- Minimize(quad_form(w, lwcovmat))

  } else if (portfolio_type == "orp") {
    # ORP: maximize Sharpe ratio = maximize (mu'w - rf) / sqrt(w'Σw)
    # Equivalent to: minimize w'Σw subject to (mu-rf)'w = c for some target c
    # We'll use the unconstrained tangency portfolio formula instead

    if (is.null(mu_vec)) {
      stop("Need expected returns for ORP")
    }

    excess_ret <- as.numeric(mu_vec) - rf

    # Direct calculation of tangency portfolio (more stable)
    inv_cov <- solve(lwcovmat)
    numerator <- inv_cov %*% excess_ret
    denominator <- as.numeric(t(rep(1, p)) %*% inv_cov %*% excess_ret)

    return(as.vector(numerator / denominator))
  }

  prob <- Problem(objective, constraints)
  result <- solve(prob)
  as.vector(result$getValue(w))
}

# 3. Rolling window backtesting
rolling_backtest <- function(input_data, window_months = 60,
                             max_long = 1, max_short = 0, rf = 0.035 / 12) {
  ret_data <- if ("date" %in% names(input_data)) {
    input_data %>% arrange(date)
  } else {
    stop("Need Date column for rolling backtest")
  }

  dates <- ret_data$date
  ret_mat <- ret_data %>% select(-date)
  n_obs <- nrow(ret_data)
  rebal_dates <- seq(window_months + 1, n_obs, by = 1)

  results_list <- map(rebal_dates, function(end_idx) {
    start_idx <- max(1, end_idx - window_months)
    train_data <- ret_mat[start_idx:end_idx, ]

    mu_vec <- train_data %>% summarize_all(list(~ mean(., na.rm = TRUE)))
    lwcovmat <- calc_lw_cov(train_data)

    # GMV: minimum variance
    gmv_weights <- calc_constrained_weights(lwcovmat,
                                            portfolio_type = "gmv",
                                            max_long = max_long,
                                            max_short = max_short)

    # ORP: maximum Sharpe ratio using historical returns
    orp_weights <- calc_constrained_weights(lwcovmat,
                                            mu_vec = mu_vec,
                                            portfolio_type = "orp",
                                            max_long = max_long,
                                            max_short = max_short,
                                            rf = rf)

    # Out-of-sample return
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
      orp_return = orp_return,
      gmv_weights = list(gmv_weights),
      orp_weights = list(orp_weights)
    )
  })

  bind_rows(results_list)
}

# 4. Performance metrics
calc_performance_metrics <- function(backtest_results) {
  perf_data <- backtest_results %>%
    filter(!is.na(gmv_return)) %>%
    mutate(
      gmv_cumret = cumprod(1 + gmv_return),
      orp_cumret = cumprod(1 + orp_return)
    )

  # Calculate metrics
  gmv_metrics <- list(
    total_return = tail(perf_data$gmv_cumret, 1) - 1,
    annualized_return = (tail(perf_data$gmv_cumret, 1)^(12 / nrow(perf_data))) - 1,
    volatility = sd(perf_data$gmv_return) * sqrt(12),
    sharpe = mean(perf_data$gmv_return) * sqrt(12) / (sd(perf_data$gmv_return) * sqrt(12))
  )

  orp_metrics <- list(
    total_return = tail(perf_data$orp_cumret, 1) - 1,
    annualized_return = (tail(perf_data$orp_cumret, 1)^(12 / nrow(perf_data))) - 1,
    volatility = sd(perf_data$orp_return) * sqrt(12),
    sharpe = mean(perf_data$orp_return) * sqrt(12) / (sd(perf_data$orp_return) * sqrt(12))
  )

  list(gmv = gmv_metrics, orp = orp_metrics, data = perf_data)
}

# 5. Weight stability analysis
calc_weight_stability <- function(backtest_results) {
  # Extract weight matrices
  gmv_weight_mat <- do.call(rbind, backtest_results$gmv_weights)
  orp_weight_mat <- do.call(rbind, backtest_results$orp_weights)


  # Calculate turnover (sum of absolute weight changes)
  gmv_turnover <- rowSums(abs(diff(gmv_weight_mat)))
  orp_turnover <- rowSums(abs(diff(orp_weight_mat)))

  # Cross-sectional standard deviation
  gmv_stability <- apply(gmv_weight_mat, 1, sd)
  orp_stability <- apply(orp_weight_mat, 1, sd)

  list(
    gmv_turnover = gmv_turnover,
    orp_turnover = orp_turnover,
    gmv_stability = gmv_stability,
    orp_stability = orp_stability
  )
}

# 6. Visualization function
plot_backtest_results <- function(performance_metrics, weight_stability) {
  perf_data <- performance_metrics$data

  # Cumulative returns plot
  p1 <- perf_data %>%
    select(date, gmv_cumret, orp_cumret) %>%
    pivot_longer(-date, names_to = "portfolio", values_to = "cumret") %>%
    ggplot(aes(x = date, y = cumret, color = portfolio)) +
    geom_line() +
    labs(title = "Cumulative Returns", y = "Cumulative Return")

  # Turnover plot
  p2 <- tibble(
    date = perf_data$date,
    GMV = weight_stability$gmv_turnover,
    ORP = weight_stability$orp_turnover
  ) %>%
    pivot_longer(-date, names_to = "portfolio", values_to = "turnover") %>%
    ggplot(aes(x = date, y = turnover, color = portfolio)) +
    geom_line() +
    labs(title = "Portfolio Turnover", y = "Monthly Turnover")

  list(returns = p1, turnover = p2)
}


portfolio_tickers <- c(
  # Technology
  "MSFT", "AAPL", "IBM", "ORCL",

  # Financial Services
  "JPM", "BAC", "WFC", "AXP",

  # Healthcare
  "JNJ", "PFE", "MRK", "UNH",

  # Consumer Discretionary
  "HD", "MCD", "DIS", "NKE",

  # Consumer Staples
  "PG", "KO", "PEP", "WMT",

  # Energy
  "XOM", "CVX", "COP",

  # Industrials
  "GE", "CAT", "BA",

  # Materials/Chemicals
  "DD",

  # Utilities
  "NEE",

  # Commodities/Resources
  "GLD",  # SPDR Gold Trust (tracks gold)
  "USO"   # United States Oil Fund (tracks oil)
)

# Usage with your existing functions:
input_df <- get_tick_returns(portfolio_tickers, bypass_date_req = TRUE)



backtest_results <- rolling_backtest(input_df, window_months = 60, max_long = 1, max_short = 0.2)
performance <- calc_performance_metrics(backtest_results)
stability <- calc_weight_stability(backtest_results)
plots <- plot_backtest_results(performance, stability)
