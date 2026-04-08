# Merge Compustat and CRSP by CUSIP
# Converted from SAS code to R using tidyverse and dbplyr
# Based on design patterns from R functions folder and project 1 empirical finance.R

# =============================================================================
# SETUP
# =============================================================================

# Load required libraries
library(tidyverse)
library(dbplyr)
library(RPostgres)
library(lubridate)

# Source the wharton connection functions
# source("../R/wharton-connections.R")

# Connect to WRDS
con <- connect_wharton()
cat("Connected to WRDS\n")

# =============================================================================
# STEP ONE: Create Linking Table with 8-digit CUSIP
# =============================================================================

cat("Step 1: Creating linking table with 8-digit CUSIP...\n")

# Create 8-digit CUSIP using "NAMES" file
# Filter for specific tickers: DELL, IBM, MSFT, F, DIS
tickers_list <- c("WMT")

compcusip <- tbl(con, in_schema("comp", "names")) %>%
  filter(tic %in% !!tickers_list) %>%
  select(gvkey, cusip, tic) %>%
  collect() %>%
  mutate(
    cusip8 = substr(cusip, 1, 8)
  )

cat("Compustat CUSIP data extracted:\n")
print(compcusip %>% select(gvkey, tic, cusip, cusip8))

# Extract CRSP CUSIP from "STOCKNAMES" file
crspcusip <- tbl(con, in_schema("crsp", "stocknames")) %>%
  select(cusip, permco, permno) %>%
  collect() %>%
  distinct(cusip, .keep_all = TRUE) %>%
  arrange(cusip)

cat("\nCRSP CUSIP data extracted:\n")
cat("  Unique CUSIPs:", nrow(crspcusip), "\n")

# Merge Compustat CUSIP with CRSP CUSIP
total <- compcusip %>%
  inner_join(
    crspcusip,
    by = c("cusip8" = "cusip")
  )

cat("\nMerged linking table:\n")
print(total %>% select(gvkey, tic, cusip8, permno))

# =============================================================================
# STEP TWO: Extract Compustat Data
# =============================================================================

cat("\nStep 2: Extracting Compustat data...\n")

# Selected GVKEYS (use quotes for character variables)
glist <- c('006066', '012141', '014489')

# Date range - applied to FYEAR (Fiscal Year)
fyear1 <- 1997
fyear2 <- 2006

# Selected data items
# Note: GVKEY, DATADATE, FYEAR and FYR are automatically included
vars <- c("gvkey", "fyr", "fyear", "datadate", "sale", "at",
          "indfmt", "datafmt", "popsrc", "consol")

# Make extract from Compustat Annual Funda file
compx2 <- tbl(con, in_schema("comp", "funda")) %>%
  select(all_of(vars)) %>%
  filter(
    gvkey %in% !!glist,
    fyear >= !!fyear1,
    fyear <= !!fyear2,
    indfmt == 'INDL',
    datafmt == 'STD',
    popsrc == 'D',
    consol == 'C'
  ) %>%
  collect() %>%
  mutate(
    # Create begin and end dates for fiscal year
    endfyr = as.Date(datadate),
    begfyr = endfyr %m-% months(11),  # intnx equivalent: subtract 11 months
    # Compute sales over assets ratio
    sxa = sale / at
  ) %>%
  # select(gvkey, begfyr, endfyr, sxa, fyr, fyear) %>%
  arrange(gvkey, endfyr)

cat("Compustat data extracted:\n")
cat("  Observations:", nrow(compx2), "\n")
cat("  Unique GVKEYs:", n_distinct(compx2$gvkey), "\n")

# Display sample data
cat("\nSample Compustat data (first 10 rows):\n")
print(head(compx2, 10))

# =============================================================================
# STEP THREE: Link GVKEYS to CRSP Identifiers
# =============================================================================

cat("\nStep 3: Linking GVKEYS to CRSP identifiers...\n")

# Merge Compustat set with linking table
mydata <- compx2 %>%
  inner_join(
    total,
    by = "gvkey"
  )

cat("Merged data:\n")
cat("  Observations:", nrow(mydata), "\n")
cat("  Unique GVKEYs:", n_distinct(mydata$gvkey), "\n")
cat("  Unique PERMNOs:", n_distinct(mydata$permno), "\n")

cat("\nSample merged data (first 10 rows):\n")
print(mydata %>% select(gvkey, permno, permco, endfyr, sxa) %>% head(10))

# =============================================================================
# STEP FOUR: Add CRSP Monthly Price Data
# =============================================================================

cat("\nStep 4: Adding CRSP monthly price data...\n")

# Option 1: Simple match at the end of the fiscal year
# Match accounting data with fiscal yearends in month 't',
# with CRSP return data from the same month 't'

mydata2 <- mydata %>%
  # Add month and year columns for matching
  mutate(
    endfyr_month = month(endfyr),
    endfyr_year = year(endfyr)
  ) %>%
  inner_join(
    tbl(con, in_schema("crsp", "msf")) %>%
      filter(!is.na(ret)) %>%
      select(permno, date, ret, prc, vol) %>%
      mutate(
        date_month = month(date),
        date_year = year(date)
      ) %>%
      collect(),
    by = c("permno" = "permno",
           "endfyr_month" = "date_month",
           "endfyr_year" = "date_year")
  )

cat("Option 1: Matched at fiscal year end\n")
cat("  Observations:", nrow(mydata2), "\n")

cat("\nSample matched data (first 10 rows):\n")
print(mydata2 %>%
      select(gvkey, permno, endfyr, date, sxa, ret) %>%
      head(10))

# =============================================================================
# OPTION 2: Alternative Matching Method
# =============================================================================

cat("\nOption 2: Alternative matching method...\n")

# Match accounting data with fiscal yearends in month 't',
# with CRSP return data from month 't+3' to month 't+14' (12 months)
# This corresponds to intck('month',a.endfyr,b.date) between 3 and 14

# First, we need to collect the CRSP data separately for this matching
crsp_monthly <- tbl(con, in_schema("crsp", "msf")) %>%
  filter(!is.na(ret)) %>%
  select(permno, date, ret, prc, vol) %>%
  collect()

# Perform the matching
mydata3 <- mydata %>%
  # Cross join with all CRSP data for matching PERMNOs
  inner_join(
    crsp_monthly %>%
      select(permno, date, ret, prc, vol),
    by = "permno"
  ) %>%
  mutate(
    # Calculate months between endfyr and date
    # This is equivalent to SAS: intck('month',a.endfyr,b.date)
    months_between = as.integer(
      (year(date) - year(endfyr)) * 12 + (month(date) - month(endfyr))
    )
  ) %>%
  filter(
    months_between >= 3,
    months_between <= 14
  )

cat("Option 2: Matched with 3-14 month lag\n")
cat("  Observations:", nrow(mydata3), "\n")

cat("\nSample alternative matched data (first 10 rows):\n")
print(mydata3 %>%
      select(gvkey, permno, endfyr, date, months_between, sxa, ret) %>%
      head(10))

# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("SUMMARY STATISTICS\n")
cat(paste(rep("=", 60), collapse = ""), "\n")

cat("\nOption 1 (Same Month Match):\n")
cat("  Total observations:", nrow(mydata2), "\n")
cat("  Unique GVKEYs:", n_distinct(mydata2$gvkey), "\n")
cat("  Unique PERMNOs:", n_distinct(mydata2$permno), "\n")
cat("  Date range:", min(mydata2$date, na.rm = TRUE), "to", max(mydata2$date, na.rm = TRUE), "\n")
cat("  Mean return:", round(mean(mydata2$ret, na.rm = TRUE), 4), "\n")
cat("  Mean sales/assets:", round(mean(mydata2$sxa, na.rm = TRUE), 4), "\n")

cat("\nOption 2 (3-14 Month Lag Match):\n")
cat("  Total observations:", nrow(mydata3), "\n")
cat("  Unique GVKEYs:", n_distinct(mydata3$gvkey), "\n")
cat("  Unique PERMNOs:", n_distinct(mydata3$permno), "\n")
cat("  Date range:", min(mydata3$date, na.rm = TRUE), "to", max(mydata3$date, na.rm = TRUE), "\n")
cat("  Mean return:", round(mean(mydata3$ret, na.rm = TRUE), 4), "\n")
cat("  Mean sales/assets:", round(mean(mydata3$sxa, na.rm = TRUE), 4), "\n")

# =============================================================================
# DATA EXPORT (Optional)
# =============================================================================

# If you want to save the data locally (equivalent to proc download in SAS):
# Uncomment the following lines to save the datasets

# write.csv(mydata2, "compustat_crsp_merged_samemonth.csv", row.names = FALSE)
# write.csv(mydata3, "compustat_crsp_merged_lag.csv", row.names = FALSE)

# Or save as RDS for faster loading in R:
# saveRDS(mydata2, "compustat_crsp_merged_samemonth.rds")
# saveRDS(mydata3, "compustat_crsp_merged_lag.rds")

# =============================================================================
# CLEANUP
# =============================================================================

# Close database connection
dbDisconnect(con)
cat("\nWRDS connection closed\n")

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("ANALYSIS COMPLETE\n")
cat(paste(rep("=", 60), collapse = ""), "\n")

# =============================================================================
# NOTES
# =============================================================================

cat("\nNotes on SAS to R conversion:\n")
cat("1. SAS 'substr()' -> R 'substr()' (same syntax)\n")
cat("2. SAS 'intnx('month',endfyr,-11,'beg')' -> R 'endfyr %m-% months(11)'\n")
cat("3. SAS 'proc sql' joins -> R 'inner_join()' from dplyr\n")
cat("4. SAS 'month()' and 'year()' functions -> R 'month()' and 'year()' from lubridate\n")
cat("5. SAS 'intck('month',a.endfyr,b.date)' -> R 'interval() %/% months(1)'\n")
cat("6. SAS 'proc sort nodupkey' -> R 'distinct()' from dplyr\n")
cat("7. SAS filtering with 'where' -> R 'filter()' from dplyr\n")
cat("8. All database queries use dbplyr for lazy evaluation and optimization\n")
