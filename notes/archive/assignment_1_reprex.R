# Assignment 1: Custom Equity Index Construction - Reproducible Example
# Based on assignment 1.R but made self-contained with WRDS package
# Author: Reproducible version of existing assignment

# =============================================================================
# SETUP AND PARAMETERS
# =============================================================================

# Clear environment
rm(list = ls())

# Install required packages if not already installed
if (!require("tidyverse", quietly = TRUE)) {
  install.packages("tidyverse")
  library(tidyverse)
}

if (!require("RPostgres", quietly = TRUE)) {
  install.packages("RPostgres")
  library(RPostgres)
}

# Source the wharton connection functions (assuming they exist)
# If not available, we'll create a simple connection function
if (file.exists("../R/wharton-connections.R")) {
  source("../R/wharton-connections.R")
} else {
  # Create a simple connection function if the file doesn't exist
  connect_wharton <- function(){
    library(RPostgres)
    dbConnect(Postgres(),
              host='wrds-pgdata.wharton.upenn.edu',
              port=9737,
              dbname='wrds',
              sslmode='require',
              user='zkg232')  # Replace with your WRDS username
  }
}

# Parameters
START_DATE <- "2000-01-01"
END_DATE <- "2020-12-31"

cat("Period:", START_DATE, "to", END_DATE, "\n")

# =============================================================================
# ORIGINAL FUNCTIONS (from assignment 1.R)
# =============================================================================

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

# =============================================================================
# MAIN ANALYSIS (following original assignment 1.R)
# =============================================================================

# Connect to WRDS
cat("Connecting to WRDS...\n")
con <- connect_wharton()
cat("Connected to WRDS\n")

# Load data
cat("Loading CRSP data...\n")
panel <- get_crsp_monthly_panel(con, START_DATE, END_DATE)
cat("Rows:", nrow(panel), "\n")

cat("Loading delisting data...\n")
delist <- get_delistings(con, START_DATE, END_DATE)
panel <- add_effective_returns(panel, delist)

cat("Data loaded successfully\n")
cat("  Total observations:", nrow(panel), "\n")
cat("  Unique securities:", length(unique(panel$permno)), "\n")
cat("  Date range:", min(panel$date), "to", max(panel$date), "\n")

# Create weight panel (lagged by 1 month for t-1 weights)
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

# Calculate custom indexes
custom_indexes <- weight_panel %>%
  inner_join(panel %>% select(date,ticker,comnam,permno,ret_eff)) %>%
  mutate(across(contains("weight"),~.*ret_eff)) %>%
  group_by(date) %>%
  summarize(across(contains("weight"),~sum(.,na.rm=TRUE))) %>%
  add_row(date = min(.$date,na.rm=TRUE) %m-% months(1),.before = 1) %>%
  replace_na(list(price_weight = 0, value_weight = 0, eq_weight = 0)) %>%
  mutate(across(contains("weight"),~cumprod(1+.)*100))

# =============================================================================
# VISUALIZATION (Base R alternative to hchart)
# =============================================================================

# Create performance plot using base R
cat("\nCreating visualizations...\n")

# Set up plotting parameters
par(mar = c(5, 4, 4, 2) + 0.1)
plot_colors <- c("blue", "red", "green")

# Create the main plot
plot(custom_indexes$date, custom_indexes$price_weight,
     type = "l", col = plot_colors[1], lwd = 2,
     xlab = "Date", ylab = "Index Level (Base=100)",
     main = "Custom Equity Index Performance",
     ylim = range(custom_indexes[, c("price_weight", "value_weight", "eq_weight")]))

# Add other index lines
lines(custom_indexes$date, custom_indexes$value_weight,
      col = plot_colors[2], lwd = 2)
lines(custom_indexes$date, custom_indexes$eq_weight,
      col = plot_colors[3], lwd = 2)

# Add legend
legend("topleft",
       legend = c("Price Weighted", "Value Weighted", "Equal Weighted"),
       col = plot_colors, lwd = 2, bty = "n")

# Add grid
grid()

# =============================================================================
# PERFORMANCE ANALYSIS
# =============================================================================

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("CUSTOM INDEX PERFORMANCE SUMMARY\n")
cat(paste(rep("=", 60), collapse = ""), "\n")

# Calculate performance metrics
for (col in c("price_weight", "value_weight", "eq_weight")) {
  index_values <- custom_indexes[[col]]
  returns <- diff(index_values) / index_values[-length(index_values)]

  cat("\n", toupper(gsub("_", " ", col)), ":\n")
  cat("  Final Level:", round(index_values[length(index_values)], 2), "\n")
  cat("  Total Return:", round((index_values[length(index_values)] / 100 - 1) * 100, 2), "%\n")
  cat("  Annualized Return:", round((mean(returns, na.rm = TRUE) + 1)^12 - 1, 4) * 100, "%\n")
  cat("  Annualized Volatility:", round(sd(returns, na.rm = TRUE) * sqrt(12) * 100, 2), "%\n")

  # Calculate Sharpe ratio (assuming 0% risk-free rate)
  sharpe_ratio <- (mean(returns, na.rm = TRUE) * 12) / (sd(returns, na.rm = TRUE) * sqrt(12))
  cat("  Sharpe Ratio:", round(sharpe_ratio, 3), "\n")

  # Calculate maximum drawdown
  running_max <- cummax(index_values)
  drawdowns <- (index_values / running_max) - 1
  max_drawdown <- min(drawdowns, na.rm = TRUE) * 100
  cat("  Max Drawdown:", round(max_drawdown, 2), "%\n")
}

# =============================================================================
# ADDITIONAL ANALYSIS
# =============================================================================

# Calculate correlations
returns_data <- data.frame(
  price_return = diff(custom_indexes$price_weight) / custom_indexes$price_weight[-nrow(custom_indexes)],
  value_return = diff(custom_indexes$value_weight) / custom_indexes$value_weight[-nrow(custom_indexes)],
  eq_return = diff(custom_indexes$eq_weight) / custom_indexes$eq_weight[-nrow(custom_indexes)]
)

cat("\nReturn Correlations:\n")
cor_matrix <- cor(returns_data, use = "complete.obs")
print(round(cor_matrix, 3))

# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

cat("\nData Summary:\n")
cat("  Total observations:", nrow(custom_indexes), "\n")
cat("  Date range:", min(custom_indexes$date), "to", max(custom_indexes$date), "\n")
cat("  Index types: Price Weighted, Value Weighted, Equal Weighted\n")

# Show first and last few values
cat("\nFirst 5 observations:\n")
print(head(custom_indexes, 5))

cat("\nLast 5 observations:\n")
print(tail(custom_indexes, 5))

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("ANALYSIS COMPLETE\n")
cat(paste(rep("=", 60), collapse = ""), "\n")

# =============================================================================
# CLEANUP
# =============================================================================

# Close database connection
dbDisconnect(con)
cat("\nWRDS connection closed\n")

# =============================================================================
# NOTES
# =============================================================================

cat("\nNotes:\n")
cat("- This is a reproducible example using real WRDS data\n")
cat("- Automatically installs required packages if not present\n")
cat("- The methodology follows the same approach as the original assignment\n")
cat("- Uses dplyr, lubridate, RPostgres, and dbplyr for data manipulation\n")
cat("- Connects to WRDS using the wharton-connections.R functions\n")
cat("- If wharton-connections.R is not found, uses a basic connection function\n")
cat("- Make sure to update the WRDS username in the connection function if needed\n")
