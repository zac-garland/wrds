library(tidyverse)
library(quantmod)
library(highcharter)

# Define major investment vehicles
tickers <- list(
  # Major Stock Indices
  stocks = c(
    "SPY",    # S&P 500
    "QQQ",    # Nasdaq 100
    "DIA",    # Dow Jones
    "IWM",    # Russell 2000
    "EFA",    # MSCI EAFE (Developed Markets)
    "EEM",    # Emerging Markets
    "VTI"     # Total US Stock Market
  ),

  # Sectors
  sectors = c(
    "XLF",    # Financials
    "XLK",    # Technology
    "XLE",    # Energy
    "XLV",    # Healthcare
    "XLI",    # Industrials
    "XLP",    # Consumer Staples
    "XLY",    # Consumer Discretionary
    "XLU",    # Utilities
    "XLRE",   # Real Estate
    "XLB",    # Materials
    "XLC"     # Communications
  ),

  # Bonds
  bonds = c(
    "TLT",    # 20+ Year Treasury
    "IEF",    # 7-10 Year Treasury
    "SHY",    # 1-3 Year Treasury
    "LQD",    # Investment Grade Corporate
    "HYG",    # High Yield Corporate
    "MUB",    # Municipal Bonds
    "TIP",    # TIPS (Inflation Protected)
    "EMB"     # Emerging Market Bonds
  ),

  # Commodities
  commodities = c(
    "GLD",    # Gold
    "SLV",    # Silver
    "USO",    # Oil
    "UNG",    # Natural Gas
    "DBA",    # Agriculture
    "PALL",   # Palladium
    "PPLT",   # Platinum
    "CPER"    # Copper
  ),

  # Real Estate
  real_estate = c(
    "VNQ",    # US Real Estate
    "VNQI"    # International Real Estate
  ),

  # Alternatives
  alternatives = c(
    "BTC-USD", # Bitcoin
    "ETH-USD", # Ethereum
    "DBC",     # Commodities Broad
    "VXX"      # Volatility (VIX)
  ),

  # Currency
  currency = c(
    "UUP",    # US Dollar Index
    "FXE",    # Euro
    "FXY"     # Japanese Yen
  )
)

# Flatten to single vector
all_tickers <- unlist(tickers, use.names = FALSE)

# Create a cleaner subset for analysis
core_tickers <- c(
  # Equities
  "SPY",    # S&P 500
  "QQQ",    # Nasdaq
  "EFA",    # International Developed
  "EEM",    # Emerging Markets,
  "PL=F",   # Platinum

  # Bonds
  "TLT",    # Long-term Treasury
  "LQD",    # Corporate Bonds
  "HYG",    # High Yield

  # Commodities
  "GLD",    # Gold
  "USO",    # Oil
  "DBC",    # Broad Commodities

  # Real Estate
  "VNQ",    # REITs

  # Alternatives
  "BTC-USD", # Bitcoin
  "UUP"      # Dollar
)

print(all_tickers)


library(quantmod)
library(tidyverse)
library(zoo)

# Function to download and prepare data
get_price_data <- function(tickers, start_date = "2020-01-01") {

  prices_list <- list()

  for (ticker in tickers) {
    tryCatch({
      cat("Downloading", ticker, "...\n")
      data <- getSymbols(ticker, from = start_date, auto.assign = FALSE)
      prices_list[[ticker]] <- Ad(data)  # Adjusted close
      Sys.sleep(0.5)  # Be nice to Yahoo
    }, error = function(e) {
      cat("Error downloading", ticker, ":", e$message, "\n")
    })
  }

  # Merge all prices
  prices <- do.call(merge, prices_list)
  colnames(prices) <- tickers[tickers %in% names(prices_list)]

  return(prices)
}

# Download data
prices <- get_price_data(core_tickers, start_date = "2019-01-01")

# Calculate returns
returns <- na.omit(ROC(prices, type = "discrete"))

# Function to calculate rolling correlation matrix
calculate_rolling_correlation <- function(returns, window = 60) {

  dates <- index(returns)
  n_assets <- ncol(returns)
  asset_names <- colnames(returns)

  # Storage for results
  correlation_data <- list()

  for (i in window:nrow(returns)) {
    # Get rolling window
    window_returns <- returns[(i - window + 1):i, ]

    # Calculate correlation matrix
    cor_matrix <- cor(window_returns, use = "pairwise.complete.obs")

    # Extract date
    date <- dates[i]

    # Convert to long format
    for (row in 1:n_assets) {
      for (col in 1:n_assets) {
        if (row != col) {  # Exclude diagonal
          correlation_data[[length(correlation_data) + 1]] <- data.frame(
            date = as.Date(date),
            asset1 = asset_names[row],
            asset2 = asset_names[col],
            correlation = cor_matrix[row, col],
            stringsAsFactors = FALSE
          )
        }
      }
    }

    if (i %% 50 == 0) cat("Processed", i, "of", nrow(returns), "\n")
  }

  # Combine all data
  correlation_df <- do.call(rbind, correlation_data)

  return(correlation_df)
}

# Calculate 60-day rolling correlations
cat("Calculating rolling correlations...\n")
rolling_corr <- calculate_rolling_correlation(returns, window = 60)

rolling_corr %>% as_tibble() %>% group_by(asset1,asset2,year = year(date)) %>% summarise(cor_avg = mean(correlation,na.rm=TRUE))-> avg_cors


get_cluster_index <- function(df)

avg_cors %>%
  filter(year == 2019) %>%
  ungroup() %>%
  mutate(across(contains("asset"),~str_replace_all(.,setNames(tick_names$name,tick_names$ticker)))) %>%
  ungroup() %>%
  select(-year) %>%
  spread(asset2,cor_avg) %>%
  column_to_rownames("asset1") -> test_mx

order <- hclust(as.dist(1-test_mx))$order

order <- test_mx[order,order] %>% colnames()

colnames(test_mx) %>%
  tibble(name = .) %>%
  mutate(order = hclust(as.dist(1-test_mx))$order) %>%
  arrange(order) %>%
  mutate(name = as_factor(name))



avg_cors %>%
  filter(year == 2019) %>%
  ungroup() %>%
  mutate(across(contains("asset"),~str_replace_all(.,setNames(tick_names$name,tick_names$ticker)))) %>%
  ungroup() %>%
  arrange(match(asset1,order)) %>%
  mutate(asset1 = as_factor(asset1)) %>%
  arrange(match(asset2,order)) %>%
  mutate(asset2 = as_factor(asset2)) %>%
  ggplot(aes(asset1, asset2, fill = cor_avg)) +
  geom_tile(color = "white", linewidth = 0.5) +
  geom_text(aes(label = sprintf("%.2f", cor_avg)), size = 3) +  # Add correlation values
  scale_fill_gradient2(
    low = "#3498db",      # Blue for negative
    mid = "#ecf0f1",      # Light gray for zero
    high = "#e74c3c",     # Red for positive
    midpoint = 0,
    limits = c(-1, 1),
    name = "Correlation"
  ) +
  coord_fixed() +  # Makes tiles square
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
    axis.title = element_blank(),
    panel.grid = element_blank()
  ) +
  labs(title = "Average Correlation Matrix - 2019",
       subtitle = "Correlation coefficients between asset pairs") -> cor_19

avg_cors %>%
  filter(year == 2025) %>%
  ungroup() %>%
  mutate(across(contains("asset"),~str_replace_all(.,setNames(tick_names$name,tick_names$ticker)))) %>%
  ungroup() %>%
  arrange(match(asset1,order)) %>%
  mutate(asset1 = as_factor(asset1)) %>%
  arrange(match(asset2,order)) %>%
  mutate(asset2 = as_factor(asset2)) %>%
  ggplot(aes(asset1, asset2, fill = cor_avg)) +
  geom_tile(color = "white", linewidth = 0.5) +
  geom_text(aes(label = sprintf("%.2f", cor_avg)), size = 3) +  # Add correlation values
  scale_fill_gradient2(
    low = "#3498db",      # Blue for negative
    mid = "#ecf0f1",      # Light gray for zero
    high = "#e74c3c",     # Red for positive
    midpoint = 0,
    limits = c(-1, 1),
    name = "Correlation"
  ) +
  coord_fixed() +  # Makes tiles square
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1),
    axis.title = element_blank(),
    panel.grid = element_blank()
  ) +
  labs(title = "Average Correlation Matrix - 2025",
       subtitle = "Correlation coefficients between asset pairs") -> cor_25


cowplot::plot_grid(cor_19,cor_25)

# Add year-month for aggregation
rolling_corr$year_month <- format(rolling_corr$date, "%Y-%m")

# Summary statistics
rolling_corr_summary <- rolling_corr %>%
  group_by(asset1, asset2) %>%
  summarise(
    mean_corr = mean(correlation, na.rm = TRUE),
    sd_corr = sd(correlation, na.rm = TRUE),
    min_corr = min(correlation, na.rm = TRUE),
    max_corr = max(correlation, na.rm = TRUE)
  )

print(head(rolling_corr_summary))


library(highcharter)

# Prepare data for motion chart
# We'll show correlation vs volatility over time

# Calculate rolling volatility
rolling_vol <- rollapply(returns, width = 60,
                         FUN = function(x) sd(x, na.rm = TRUE) * sqrt(252) * 100,
                         by.column = TRUE, align = "right", fill = NA)

# Convert to dataframe
vol_df <- data.frame(
  date = index(rolling_vol),
  rolling_vol
) %>%
  pivot_longer(-date, names_to = "asset", values_to = "volatility") %>%
  filter(!is.na(volatility))

# Select a few key correlations for the motion chart
key_pairs <- data.frame(
  asset1 = c("SPY", "SPY", "SPY", "GLD", "TLT", "QQQ"),
  asset2 = c("TLT", "GLD", "PALL", "PALL", "SPY", "TLT")
)

# Filter correlation data for these pairs
motion_data <- rolling_corr %>%
  inner_join(key_pairs, by = c("asset1", "asset2")) %>%
  mutate(pair = paste(asset1, "vs", asset2))

# Add volatility for asset1
motion_data <- motion_data %>%
  left_join(
    vol_df %>% rename(asset1 = asset, vol1 = volatility),
    by = c("date", "asset1")
  ) %>%
  left_join(
    vol_df %>% rename(asset2 = asset, vol2 = volatility),
    by = c("date", "asset2")
  ) %>%
  mutate(avg_vol = (vol1 + vol2) / 2)

# Sample every 10 days to reduce data size
motion_data_sampled <- motion_data %>%
  arrange(date) %>%
  group_by(pair) %>%
  slice(seq(1, n(), by = 10)) %>%
  ungroup()

# Create motion chart data structure
create_motion_data <- function(df) {

  pairs <- unique(df$pair)

  series_list <- lapply(pairs, function(p) {
    pair_data <- df %>%
      filter(pair == p) %>%
      arrange(date)

    # Create sequence for each time point
    sequence <- lapply(1:nrow(pair_data), function(i) {
      list(
        x = round(pair_data$correlation[i], 3),
        y = round(pair_data$avg_vol[i], 2),
        name = format(pair_data$date[i], "%Y-%m-%d"),
        z = i
      )
    })

    list(
      name = p,
      data = sequence
    )
  })

  return(series_list)
}

motion_series <- create_motion_data(motion_data_sampled)

# Create the motion chart
highchart() %>%
  hc_chart(type = "scatter") %>%
  hc_add_series_list(motion_series) %>%

  hc_motion(
    enabled = TRUE,
    labels = unique(format(motion_data_sampled$date, "%Y-%m-%d")),
    series = 0:length(motion_series),
    updateInterval = 100,
    magnet = list(
      round = "floor",
      step = 1
    )
  ) %>%

  hc_xAxis(
    title = list(text = "Correlation"),
    min = -1,
    max = 1,
    gridLineWidth = 1,
    plotLines = list(
      list(
        value = 0,
        color = "black",
        dashStyle = "dash",
        width = 2
      )
    )
  ) %>%

  hc_yAxis(
    title = list(text = "Average Volatility (%)"),
    min = 0
  ) %>%

  hc_tooltip(
    useHTML = TRUE,
    headerFormat = "<b>{series.name}</b><br/>",
    pointFormat = "Date: {point.name}<br/>Correlation: {point.x:.2f}<br/>Volatility: {point.y:.2f}%"
  ) %>%

  hc_title(text = "Rolling Correlation vs Volatility Over Time") %>%
  hc_subtitle(text = "60-day rolling window | Click play to animate") %>%

  hc_plotOptions(
    series = list(
      marker = list(
        enabled = TRUE,
        radius = 8
      )
    )
  )
