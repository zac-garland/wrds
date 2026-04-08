library(ompr)
library(ompr.roi)
library(ROI.plugin.glpk)
library(dplyr)
library(tibble)
library(readr)

load_and_process_data <- function(csv_file) {
  # Load CSV data and convert prices to returns

  cat("Reading data from:", csv_file, "\n")
  df <- read_csv(csv_file, show_col_types = FALSE)

  cat("Data shape:", nrow(df), "x", ncol(df), "\n")
  cat("Columns:", paste(colnames(df), collapse = ", "), "\n")
  cat("First few rows:\n")
  print(head(df))
  cat("\n")

  # Extract dates (first column)
  date_column <- colnames(df)[1]
  dates <- df[[date_column]]

  # Get stock columns (exclude first column and NDX if present)
  all_columns <- colnames(df)
  stock_columns <- all_columns[-1]  # Remove first column (dates)
  stock_columns <- stock_columns[toupper(stock_columns) != "NDX"]  # Remove NDX

  cat("Date column:", date_column, "\n")
  cat("Stock columns (", length(stock_columns), "):", paste(stock_columns, collapse = ", "), "\n")
  cat("Excluded columns: NDX (if present)\n\n")

  # Extract price data
  price_data <- as.matrix(df[, stock_columns])
  n_periods <- nrow(price_data)
  n_stocks <- ncol(price_data)

  cat("Price data shape:", n_periods, "x", n_stocks, "\n")
  cat("Price data preview:\n")
  print(price_data[1:min(5, n_periods), 1:min(5, n_stocks)])
  cat("\n")

  # Calculate returns: (P_t - P_{t-1}) / P_{t-1}
  returns_data <- matrix(0, nrow = n_periods - 1, ncol = n_stocks)
  colnames(returns_data) <- stock_columns

  for(i in 2:n_periods) {
    for(j in 1:n_stocks) {
      if(price_data[i-1, j] != 0) {
        returns_data[i-1, j] <- (price_data[i, j] - price_data[i-1, j]) / price_data[i-1, j]
      } else {
        returns_data[i-1, j] <- 0  # Handle zero price
      }
    }
  }

  # Remove first date since no return calculated
  return_dates <- dates[-1]

  cat("Returns data shape:", nrow(returns_data), "x", ncol(returns_data), "\n")
  cat("Returns data preview:\n")
  print(returns_data[1:min(5, nrow(returns_data)), 1:min(5, ncol(returns_data))])
  cat("Return bounds: Min =", min(returns_data), ", Max =", max(returns_data), "\n\n")

  return(list(
    returns_data = returns_data,
    stock_names = stock_columns,
    dates = return_dates
  ))
}

solve_cvar_portfolio <- function(returns_data, stock_names, beta = 0.95, R = 0.0002,
                                 allow_short = FALSE, max_leverage = 1.0, verbose = TRUE) {
  # Solve CVaR portfolio optimization

  n_periods <- nrow(returns_data)
  n_stocks <- ncol(returns_data)
  mean_returns <- colMeans(returns_data)

  if(verbose) {
    cat("Portfolio optimization setup:\n")
    cat("  Number of periods:", n_periods, "\n")
    cat("  Number of stocks:", n_stocks, "\n")
    cat("  Beta (confidence level):", beta, "\n")
    cat("  Minimum return requirement:", sprintf("%.6f", R), "(", sprintf("%.4f", R*100), "%)\n")
    cat("  Allow short selling:", allow_short, "\n\n")
  }

  # Create OMPR model
  model <- MIPModel() %>%
    # Decision variables
    add_variable(x[i], i = 1:n_stocks, type = "continuous",
                 lb = if(allow_short) -max_leverage else 0,
                 ub = if(allow_short) max_leverage else 1) %>%
    add_variable(alpha, type = "continuous",
                 lb = -Inf,
                 ub = if(allow_short) Inf else 1.0) %>%  # Bound VaR for long-only
    add_variable(u[k], k = 1:n_periods, type = "continuous", lb = 0) %>%

    # Objective: Minimize CVaR
    set_objective(alpha + (1/(1-beta)) * (1/n_periods) * sum_expr(u[k], k = 1:n_periods), "min") %>%

    # Portfolio weights constraint
    add_constraint(sum_expr(x[i], i = 1:n_stocks) == 1) %>%

    # Expected return constraint
    add_constraint(sum_expr(x[i] * mean_returns[i], i = 1:n_stocks) >= R)

  # Add CVaR linearization constraints
  for(k in 1:n_periods) {
    model <- model %>%
      add_constraint(u[k] >= -sum_expr(x[i] * returns_data[k, i], i = 1:n_stocks) - alpha)
  }

  if(verbose) cat("Solving optimization model...\n")

  # Solve
  result <- solve_model(model, with_ROI(solver = "glpk"))

  return(list(model = model, result = result, mean_returns = mean_returns))
}

analyze_portfolio <- function(model_result, returns_data, stock_names, beta) {
  # Analyze and display portfolio results

  result <- model_result$result
  mean_returns <- model_result$mean_returns

  if(result$status != "success") {
    cat("Optimization failed with status:", result$status, "\n")
    return(NULL)
  }

  n_periods <- nrow(returns_data)
  n_stocks <- ncol(returns_data)

  # Extract solution
  optimal_weights <- get_solution(result, x[i])$value
  optimal_alpha <- get_solution(result, alpha)$value
  optimal_cvar <- objective_value(result)

  cat(paste(rep("=", 70), collapse = ""), "\n")
  cat("PORTFOLIO OPTIMIZATION RESULTS\n")
  cat(paste(rep("=", 70), collapse = ""), "\n\n")

  cat("Objective Value (CVaR):", sprintf("%.6f", optimal_cvar), "\n")
  cat("VaR (α):", sprintf("%.6f", optimal_alpha), "\n")

  cat("\nOptimal Portfolio Weights:\n")
  cat(paste(rep("-", 40), collapse = ""), "\n")

  # Create results data frame
  portfolio_df <- tibble(
    Stock = stock_names,
    Weight = optimal_weights,
    `Weight %` = optimal_weights * 100,
    `Mean Return` = mean_returns,
    `Mean Return %` = mean_returns * 100
  ) %>%
    filter(abs(Weight) > 0.001) %>%  # Only significant holdings
    arrange(desc(abs(Weight)))

  print(portfolio_df)

  total_weight <- sum(optimal_weights)
  cat("\nTotal weight:", sprintf("%.4f", total_weight), "(", sprintf("%.2f", total_weight*100), "%)\n")
  cat("Number of stocks with significant holdings (>0.1%):", nrow(portfolio_df), "\n")

  # Portfolio statistics
  portfolio_returns <- as.vector(returns_data %*% optimal_weights)
  mean_return <- mean(portfolio_returns)
  std_return <- sd(portfolio_returns)
  min_return <- min(portfolio_returns)
  max_return <- max(portfolio_returns)

  cat("\nPortfolio Statistics:\n")
  cat(paste(rep("-", 30), collapse = ""), "\n")
  cat("Expected daily return:", sprintf("%.6f", mean_return), "(", sprintf("%.4f", mean_return*100), "%)\n")
  cat("Daily volatility (std):", sprintf("%.6f", std_return), "(", sprintf("%.4f", std_return*100), "%)\n")
  cat("Minimum daily return: ", sprintf("%.6f", min_return), "(", sprintf("%.4f", min_return*100), "%)\n")
  cat("Maximum daily return: ", sprintf("%.6f", max_return), "(", sprintf("%.4f", max_return*100), "%)\n")

  # Annualized statistics (252 trading days)
  annualized_return <- mean_return * 252
  annualized_vol <- std_return * sqrt(252)
  sharpe_ratio <- if(annualized_vol > 0) annualized_return / annualized_vol else 0

  cat("\nAnnualized Statistics (252 trading days):\n")
  cat(paste(rep("-", 40), collapse = ""), "\n")
  cat("Expected annual return:", sprintf("%.4f", annualized_return), "(", sprintf("%.2f", annualized_return*100), "%)\n")
  cat("Annual volatility:     ", sprintf("%.4f", annualized_vol), "(", sprintf("%.2f", annualized_vol*100), "%)\n")
  cat("Sharpe ratio:          ", sprintf("%.4f", sharpe_ratio), "\n")

  # VaR/CVaR analysis
  sorted_returns <- sort(portfolio_returns)
  var_index <- floor((1-beta) * length(sorted_returns)) + 1
  empirical_var <- -sorted_returns[var_index]
  empirical_cvar <- -mean(sorted_returns[1:var_index])

  cat("\nRisk Analysis:\n")
  cat(paste(rep("-", 20), collapse = ""), "\n")
  cat("Model VaR:     ", sprintf("%.6f", optimal_alpha), "(", sprintf("%.4f", optimal_alpha*100), "%)\n")
  cat("Empirical VaR: ", sprintf("%.6f", empirical_var), "(", sprintf("%.4f", empirical_var*100), "%)\n")
  cat("Model CVaR:    ", sprintf("%.6f", optimal_cvar), "(", sprintf("%.4f", optimal_cvar*100), "%)\n")
  cat("Empirical CVaR:", sprintf("%.6f", empirical_cvar), "(", sprintf("%.4f", empirical_cvar*100), "%)\n")

  return(list(
    weights = optimal_weights,
    var = optimal_alpha,
    cvar = optimal_cvar,
    portfolio_returns = portfolio_returns,
    mean_return = mean_return,
    volatility = std_return,
    sharpe_ratio = sharpe_ratio,
    portfolio_df = portfolio_df
  ))
}

# ===== MAIN EXECUTION =====

# IMPORTANT: This is where we read the CSV file
csv_file <- path.expand("~/./Downloads/stocks2019.csv")  # <-- CHANGE THIS TO YOUR ACTUAL FILE PATH

# Load and process data
tryCatch({
  data_result <- load_and_process_data(csv_file)
  returns_data <- data_result$returns_data
  stock_names <- data_result$stock_names
  dates <- data_result$dates
}, error = function(e) {
  cat("Error: Could not find or process file '", csv_file, "'\n")
  cat("Please make sure the file exists in the current directory\n")
  cat("or provide the full path to the file.\n")
  cat("Error details:", e$message, "\n")
  stop("Data loading failed")
})

# Problem parameters (from assignment)
beta <- 0.95      # 95% confidence level
R <- 0.02/100     # 0.02% minimum daily return

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STARTING PORTFOLIO OPTIMIZATION\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

# Solve optimization
model_result <- solve_cvar_portfolio(
  returns_data = returns_data,
  stock_names = stock_names,
  beta = beta,
  R = R,
  allow_short = FALSE,  # Long-only portfolio
  verbose = TRUE
)

# Analyze results
portfolio_results <- analyze_portfolio(model_result, returns_data, stock_names, beta)

if(!is.null(portfolio_results)) {
  cat("\n", paste(rep("=", 70), collapse = ""), "\n")
  cat("OPTIMIZATION COMPLETED SUCCESSFULLY\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  cat("Portfolio optimized using", length(dates), "days of data\n")
  cat("from", as.character(dates[1]), "to", as.character(dates[length(dates)]), "\n")
} else {
  cat("Optimization failed!\n")
}
