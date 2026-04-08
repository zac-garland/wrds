# Step-by-step merge test to identify where issues occur
# This helps debug merge problems

library(tidyverse)
library(dbplyr)
library(RPostgres)

# Source the functions
devtools::load_all()

# Connect to WRDS
cat("Connecting to WRDS...\n")
con <- connect_wharton()
con <- ensure_connection(con)
cat("Connection verified!\n\n")

# Test parameters
start_year <- 2000
end_year <- 2000
start_date <- "2000-01-01"
end_date <- "2000-12-31"
tickers <- c("IBM", "MSFT")

cat("Test Parameters:\n")
cat("  Start Year:", start_year, "\n")
cat("  End Year:", end_year, "\n")
cat("  Start Date:", start_date, "\n")
cat("  End Date:", end_date, "\n")
cat("  Tickers:", paste(tickers, collapse = ", "), "\n\n")

# Step 1: Create linking table
cat(paste(rep("=", 60), collapse = ""), "\n")
cat("STEP 1: Creating linking table\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
tryCatch({
  link_table <- create_compustat_crsp_link(con, tickers = tickers, collect = FALSE)
  cat("✓ Linking table created (lazy)\n")

  # Check if we can see the structure
  cat("  Checking linking table structure...\n")
  link_sample <- link_table %>% head(1) %>% collect()
  cat("  ✓ Can access linking table\n")
  cat("  Sample row:\n")
  print(link_sample)
}, error = function(e) {
  cat("✗ Error creating linking table:\n")
  print(e)
  stop("Cannot proceed without linking table")
})

# Step 2: Get Compustat data
cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("STEP 2: Getting Compustat data\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
tryCatch({
  comp_data <- get_compustat_funda(
    con = con,
    start_year = start_year,
    end_year = end_year,
    apply_filters = TRUE,
    collect = FALSE
  )
  cat("✓ Compustat data retrieved (lazy)\n")

  # Check if we can see the structure
  cat("  Checking Compustat data structure...\n")
  comp_sample <- comp_data %>% head(1) %>% collect()
  cat("  ✓ Can access Compustat data\n")
  cat("  Sample row:\n")
  print(comp_sample)
  cat("  Total rows (if collected):", comp_data %>% count() %>% collect() %>% pull(n), "\n")
}, error = function(e) {
  cat("✗ Error getting Compustat data:\n")
  print(e)
  stop("Cannot proceed without Compustat data")
})

# Step 3: Get CRSP data
cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("STEP 3: Getting CRSP data\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
tryCatch({
  crsp_data <- get_crsp_msf(
    con = con,
    start_date = start_date,
    end_date = end_date,
    apply_filters = TRUE,
    join_msenames = TRUE,
    collect = FALSE
  )
  cat("✓ CRSP data retrieved (lazy)\n")

  # Check if we can see the structure
  cat("  Checking CRSP data structure...\n")
  crsp_sample <- crsp_data %>% head(1) %>% collect()
  cat("  ✓ Can access CRSP data\n")
  cat("  Sample row:\n")
  print(crsp_sample)
  cat("  Total rows (if collected):", crsp_data %>% count() %>% collect() %>% pull(n), "\n")
}, error = function(e) {
  cat("✗ Error getting CRSP data:\n")
  print(e)
  stop("Cannot proceed without CRSP data")
})

# Step 4: Merge manually
cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("STEP 4: Merging datasets manually\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
tryCatch({
  # First join: Compustat with linking table
  cat("  4a. Joining Compustat with linking table...\n")
  comp_linked <- comp_data %>%
    inner_join(link_table, by = "gvkey")

  comp_linked_sample <- comp_linked %>% head(1) %>% collect()
  cat("  ✓ Compustat linked successfully\n")
  cat("  Sample row:\n")
  print(comp_linked_sample)

  # Second join: Add month/year for same_month merge
  cat("\n  4b. Adding month/year columns...\n")
  comp_linked <- comp_linked %>%
    mutate(
      comp_month = month(datadate),
      comp_year = year(datadate)
    )

  crsp_data <- crsp_data %>%
    mutate(
      crsp_month = month(date),
      crsp_year = year(date)
    )
  cat("  ✓ Month/year columns added\n")

  # Third join: Merge with CRSP
  cat("\n  4c. Joining with CRSP data...\n")
  merged <- comp_linked %>%
    inner_join(
      crsp_data,
      by = c("permno" = "permno",
             "comp_month" = "crsp_month",
             "comp_year" = "crsp_year")
    ) %>%
    # Only remove comp_month and comp_year (crsp_month/crsp_year don't exist after join)
    select(-comp_month, -comp_year)

  cat("  ✓ Join completed (lazy)\n")

  # Final collection
  cat("\n  4d. Collecting final result...\n")
  merged_final <- merged %>% collect()

  cat("  ✓ Collection complete!\n")
  cat("\nFinal Results:\n")
  cat("  Rows:", nrow(merged_final), "\n")
  cat("  Columns:", ncol(merged_final), "\n")

  if (nrow(merged_final) > 0) {
    cat("\nFirst few rows:\n")
    print(head(merged_final, 3))
  } else {
    cat("\n  Note: No matching records found.\n")
    cat("  This could mean:\n")
    cat("  - No overlap in dates between Compustat and CRSP\n")
    cat("  - No matching PERMNOs in the date range\n")
    cat("  - Filters excluded all records\n")
  }

}, error = function(e) {
  cat("✗ Error during merge:\n")
  cat("  Error message:", e$message, "\n")
  cat("\n  Full error:\n")
  print(e)
  cat("\n  Traceback:\n")
  print(traceback())
})

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("Step-by-step test complete!\n")
cat(paste(rep("=", 60), collapse = ""), "\n")

# Close connection
dbDisconnect(con)
cat("\nConnection closed.\n")

