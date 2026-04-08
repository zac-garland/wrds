# Generalized Functions for Merging Compustat and CRSP Data
# Based on design patterns from wharton-connections.R and Assignment 0.pdf
# Provides reusable functions for linking Compustat and CRSP datasets with common filters

#' Check if database connection is still valid
#'
#' @param con Database connection object
#' @return Logical, TRUE if connection is valid, FALSE otherwise
#'
#' @export
check_connection <- function(con) {
  if (is.null(con)) return(FALSE)
  tryCatch({
    # Check if connection object is valid
    if (!DBI::dbIsValid(con)) {
      return(FALSE)
    }
    # Try a very simple query
    DBI::dbGetQuery(con, "SELECT 1")
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

#' Reconnect to WRDS if connection is invalid
#'
#' @param con Current connection object (may be NULL or invalid)
#' @return Valid database connection
#'
#' @export
ensure_connection <- function(con = NULL) {
  if (is.null(con) || !check_connection(con)) {
    cat("Connection invalid or expired. Reconnecting to WRDS...\n")
    con <- connect_wharton()
    cat("Reconnected successfully!\n")
  }
  return(con)
}

#' Create linking table between Compustat and CRSP using CUSIP
#'
#' @param con Database connection object (from connect_wharton())
#' @param tickers Optional vector of ticker symbols to filter by (e.g., c("IBM", "MSFT"))
#' @param gvkeys Optional vector of GVKEYs to filter by
#' @param collect Logical, whether to collect the result immediately (default: FALSE for efficiency)
#' @return A dataframe or lazy tbl with linking information (gvkey, cusip8, permno, permco, ticker)
#'
#' @export
create_compustat_crsp_link <- function(con, tickers = NULL, gvkeys = NULL, collect = FALSE) {

  # Check connection is still valid
  if (!check_connection(con)) {
    stop("Database connection appears to be closed or timed out. Please reconnect using connect_wharton() or use ensure_connection()")
  }

  # Get Compustat CUSIP data from names file
  # Use tryCatch to handle connection issues gracefully
  compcusip <- tryCatch({
    tbl(con, in_schema("comp", "names"))
  }, error = function(e) {
    if (grepl("timeout|timed out", e$message, ignore.case = TRUE)) {
      stop("Database query timed out. The connection may have expired. Please reconnect using connect_wharton()")
    } else {
      stop("Error accessing comp.names table: ", e$message)
    }
  })

  # Apply filters if provided
  if (!is.null(tickers)) {
    compcusip <- compcusip %>% filter(tic %in% !!tickers)
  }
  if (!is.null(gvkeys)) {
    compcusip <- compcusip %>% filter(gvkey %in% !!gvkeys)
  }

  # Create 8-digit CUSIP and select relevant columns
  compcusip <- compcusip %>%
    select(gvkey, cusip, tic) %>%
    mutate(cusip8 = substr(cusip, 1, 8))

  # Get CRSP CUSIP data from stocknames file
  crspcusip <- tbl(con, in_schema("crsp", "stocknames")) %>%
    select(cusip, permco, permno) %>%
    distinct(cusip, .keep_all = TRUE)

  # Merge Compustat and CRSP by CUSIP
  link_table <- compcusip %>%
    inner_join(crspcusip, by = c("cusip8" = "cusip"))

  if (collect) {
    link_table <- link_table %>% collect()
  }

  return(link_table)
}

#' Apply standard Compustat filters based on Assignment 0.pdf
#'
#' @param comp_data Lazy tbl or dataframe with Compustat data
#' @param exclude_financials Logical, exclude SIC 6000-6999 (default: TRUE)
#' @param exclude_pharma Logical, exclude SIC 2834 (default: TRUE)
#' @param currency_filter Logical, filter for USD currency (default: TRUE)
#' @param fic_filter Logical, filter for USA (default: TRUE)
#' @param exchange_filter Logical, filter for exchanges 11-19 (default: TRUE)
#' @param currency_col Name of currency column (default: "curcd")
#' @param fic_col Name of FIC column (default: "fic")
#' @param sich_col Name of SIC column (default: "sich")
#' @param exchg_col Name of exchange column (default: "exchg")
#' @return Filtered Compustat data
#'
#' @export
filter_compustat <- function(comp_data,
                             exclude_financials = TRUE,
                             exclude_pharma = TRUE,
                             currency_filter = TRUE,
                             fic_filter = TRUE,
                             exchange_filter = TRUE,
                             currency_col = "curcd",
                             fic_col = "fic",
                             sich_col = "sich",
                             exchg_col = "exchg") {

  filtered <- comp_data

  # Currency filter: USD only
  if (currency_filter) {
    filtered <- tryCatch({
      filtered %>% filter(!!sym(currency_col) == "USD")
    }, error = function(e) {
      warning(paste("Currency filter skipped:", currency_col, "column may not exist"))
      filtered
    })
  }

  # FIC filter: USA only
  if (fic_filter) {
    filtered <- tryCatch({
      filtered %>% filter(!!sym(fic_col) == "USA")
    }, error = function(e) {
      warning(paste("FIC filter skipped:", fic_col, "column may not exist"))
      filtered
    })
  }

  # Exclude financials: SIC 6000-6999
  if (exclude_financials) {
    filtered <- tryCatch({
      filtered %>% filter(!between(!!sym(sich_col), 6000, 6999))
    }, error = function(e) {
      warning(paste("Financial exclusion filter skipped:", sich_col, "column may not exist"))
      filtered
    })
  }

  # Exclude pharmaceuticals: SIC 2834
  if (exclude_pharma) {
    filtered <- tryCatch({
      filtered %>% filter(!!sym(sich_col) != 2834)
    }, error = function(e) {
      warning(paste("Pharma exclusion filter skipped:", sich_col, "column may not exist"))
      filtered
    })
  }

  # Exchange filter: 11-19
  if (exchange_filter) {
    filtered <- tryCatch({
      filtered %>% filter(between(!!sym(exchg_col), 11, 19))
    }, error = function(e) {
      warning(paste("Exchange filter skipped:", exchg_col, "column may not exist"))
      filtered
    })
  }

  return(filtered)
}

#' Apply standard CRSP filters based on Assignment 0.pdf
#'
#' @param crsp_data Lazy tbl or dataframe with CRSP data
#' @param exclude_financials Logical, exclude SIC 6000-6999 (default: TRUE)
#' @param exclude_pharma Logical, exclude SIC 2834 (default: TRUE)
#' @param remove_na_returns Logical, remove NA returns (default: TRUE)
#' @param remove_letter_returns Logical, remove returns with letters (default: TRUE)
#' @param remove_extreme_returns Logical, remove returns < -100 (default: TRUE)
#' @param ret_col Name of return column (default: "ret")
#' @param sich_col Name of SIC column (default: "siccd" for CRSP, "sich" for Compustat)
#' @return Filtered CRSP data
#'
#' @export
filter_crsp <- function(crsp_data,
                        exclude_financials = TRUE,
                        exclude_pharma = TRUE,
                        remove_na_returns = TRUE,
                        remove_letter_returns = TRUE,
                        remove_extreme_returns = TRUE,
                        ret_col = "ret",
                        sich_col = "siccd") {

  filtered <- crsp_data

  # Remove NA returns
  if (remove_na_returns) {
    filtered <- tryCatch({
      filtered %>% filter(!is.na(!!sym(ret_col)))
    }, error = function(e) {
      warning(paste("NA return filter skipped:", ret_col, "column may not exist"))
      filtered
    })
  }

  # Remove returns with letters (character returns)
  # Note: In CRSP, returns should be numeric in the database.
  # Character returns are typically filtered out at collection time.
  # This filter assumes returns are already numeric.
  # If working with collected data that has character returns,
  # filter after collection using: filter(!grepl("[A-Za-z]", ret))
  if (remove_letter_returns && is.data.frame(filtered)) {
    filtered <- tryCatch({
      # Check for character returns (only works on collected data)
      filtered %>%
        filter(!grepl("[A-Za-z]", !!sym(ret_col), ignore.case = TRUE))
    }, error = function(e) {
      warning(paste("Letter return filter skipped - may already be filtered"))
      filtered
    })
  } else if (remove_letter_returns) {
    # For lazy tbls, assume returns are numeric and will be filtered at collection
    warning("Letter return filter works best on collected data. Character returns should be filtered after collection.")
  }

  # Remove extreme returns (< -100)
  if (remove_extreme_returns) {
    filtered <- tryCatch({
      filtered %>% filter(as.numeric(!!sym(ret_col)) >= -100)
    }, error = function(e) {
      warning(paste("Extreme return filter skipped:", ret_col, "column may not exist"))
      filtered
    })
  }

  # Exclude financials: SIC 6000-6999
  # Note: CRSP uses 'siccd', Compustat uses 'sich'
  if (exclude_financials) {
    filtered <- tryCatch({
      filtered %>% filter(!between(!!sym(sich_col), 6000, 6999))
    }, error = function(e) {
      # Try alternative column names (siccd for CRSP, sich for Compustat)
      alt_col <- if (sich_col == "siccd") "sich" else "siccd"
      tryCatch({
        filtered %>% filter(!between(!!sym(alt_col), 6000, 6999))
      }, error = function(e2) {
        warning(paste("Financial exclusion filter skipped: neither", sich_col, "nor", alt_col, "column exists"))
        filtered
      })
    })
  }

  # Exclude pharmaceuticals: SIC 2834
  if (exclude_pharma) {
    filtered <- tryCatch({
      filtered %>% filter(!!sym(sich_col) != 2834)
    }, error = function(e) {
      # Try alternative column names
      alt_col <- if (sich_col == "siccd") "sich" else "siccd"
      tryCatch({
        filtered %>% filter(!!sym(alt_col) != 2834)
      }, error = function(e2) {
        warning(paste("Pharma exclusion filter skipped: neither", sich_col, "nor", alt_col, "column exists"))
        filtered
      })
    })
  }

  return(filtered)
}

#' Get Compustat fundamental data with standard filters
#'
#' Gets all fundamentals data and joins with company names for identifiers.
#'
#' @param con Database connection object
#' @param start_year Starting fiscal year (default: NULL, no filter)
#' @param end_year Ending fiscal year (default: NULL, no filter)
#' @param gvkeys Optional vector of GVKEYs to filter by
#' @param apply_filters Logical, apply standard Compustat filters (default: TRUE)
#' @param join_names Logical, join with comp.names to get company identifiers (default: TRUE)
#' @param collect Logical, whether to collect immediately (default: FALSE)
#' @return Compustat fundamental data with all columns plus company names
#'
#' @export
get_compustat_funda <- function(con,
                                start_year = NULL,
                                end_year = NULL,
                                gvkeys = NULL,
                                apply_filters = TRUE,
                                join_names = TRUE,
                                collect = FALSE) {

  # Start with comp.funda table - get ALL columns (no variable selection)
  funda_data <- tbl(con, in_schema("comp", "funda"))

  # Join with comp.names to get additional company identifiers
  # Note: funda already has tic, conm, cusip, so we only get additional fields from names
  # We get: cik, sic, naics, gsubind, gind (sector/industry codes)
  if (join_names) {
    # Get additional identifiers from names
    # Note: comp.names may have multiple records per gvkey (historical)
    # We'll join and may get some duplicates, but that's acceptable
    names_data <- tbl(con, in_schema("comp", "names")) %>%
      select(gvkey, cik, sic, naics, gsubind, gind)

    # Join to add sector/industry codes
    # Using left_join to keep all funda records even if no match in names
    funda_data <- funda_data %>%
      left_join(names_data, by = "gvkey")
  }

  # Apply date filters
  if (!is.null(start_year)) {
    funda_data <- funda_data %>% filter(fyear >= !!start_year)
  }
  if (!is.null(end_year)) {
    funda_data <- funda_data %>% filter(fyear <= !!end_year)
  }

  # Apply GVKEY filter
  if (!is.null(gvkeys)) {
    funda_data <- funda_data %>% filter(gvkey %in% !!gvkeys)
  }

  # Apply standard Compustat filters (Assignment 0.pdf)
  if (apply_filters) {
    funda_data <- filter_compustat(funda_data)

    # Additional standard Compustat filters for funda
    funda_data <- funda_data %>%
      filter(
        indfmt == 'INDL',
        datafmt == 'STD',
        popsrc == 'D',
        consol == 'C'
      )
  }

  if (collect) {
    funda_data <- funda_data %>% collect()
  }

  return(funda_data)
}

#' Get CRSP monthly stock file data with standard filters
#'
#' @param con Database connection object
#' @param start_date Starting date (default: NULL, no filter)
#' @param end_date Ending date (default: NULL, no filter)
#' @param permnos Optional vector of PERMNOs to filter by
#' @param vars Vector of variable names to select (default: common variables)
#' @param apply_filters Logical, apply standard CRSP filters (default: TRUE)
#' @param join_msenames Logical, join with msenames to get SIC codes for filtering (default: FALSE)
#' @param collect Logical, whether to collect immediately (default: FALSE)
#' @return CRSP monthly stock file data
#'
#' @export
get_crsp_msf <- function(con,
                        start_date = NULL,
                        end_date = NULL,
                        permnos = NULL,
                        vars = c("permno", "date", "ret", "prc", "vol"),
                        apply_filters = TRUE,
                        join_msenames = FALSE,
                        collect = FALSE) {

  # Start with crsp.msf table
  msf_data <- tbl(con, in_schema("crsp", "msf"))

  # Join with msenames if SIC filtering is needed
  # Note: SIC codes might be in msenames or stocknames
  # In CRSP, SIC codes are typically called 'siccd' in msenames, not 'sich'
  if (join_msenames || apply_filters) {
    # Try to get SIC code - it might be siccd in msenames
    # First, try with siccd (standard CRSP column name)
    msenames_cols <- c("permno", "namedt", "nameendt")

    # Check if we need SIC for filtering - try siccd first (CRSP standard)
    if (apply_filters) {
      # In CRSP, SIC is typically 'siccd', not 'sich'
      # sich is Compustat terminology
      msenames_cols <- c(msenames_cols, "siccd")
    }

    msf_data <- msf_data %>%
      inner_join(
        tbl(con, in_schema("crsp", "msenames")) %>%
          select(any_of(msenames_cols)),
        by = "permno"
      ) %>%
      filter(
        date >= namedt,
        date <= coalesce(nameendt, as.Date("9999-12-31"))
      )

    # Add siccd to vars if not already there (for filtering)
    if (apply_filters && !"siccd" %in% vars) {
      vars <- c(vars, "siccd")
    }
  }

  # Select variables
  msf_data <- msf_data %>% select(any_of(vars))

  # Apply date filters
  if (!is.null(start_date)) {
    msf_data <- msf_data %>% filter(date >= !!as.Date(start_date))
  }
  if (!is.null(end_date)) {
    msf_data <- msf_data %>% filter(date <= !!as.Date(end_date))
  }

  # Apply PERMNO filter
  if (!is.null(permnos)) {
    msf_data <- msf_data %>% filter(permno %in% !!permnos)
  }

  # Apply standard CRSP filters (Assignment 0.pdf)
  if (apply_filters) {
    msf_data <- filter_crsp(msf_data)
  }

  if (collect) {
    msf_data <- msf_data %>% collect()
  }

  return(msf_data)
}

#' Merge Compustat and CRSP data using the linking table
#'
#' All operations are performed in the database for efficiency. Only collects at the end if collect=TRUE.
#'
#' @param comp_data Compustat data (must be lazy tbl for in-database operations)
#' @param crsp_data CRSP data (must be lazy tbl for in-database operations)
#' @param link_table Linking table with gvkey and permno (must be lazy tbl, will be created if not provided)
#' @param con Database connection (required if link_table is NULL)
#' @param merge_method Character, how to match dates: "fiscal_year_period", "same_month", "lag_3_14", or "exact" (default: "fiscal_year_period")
#'   - "fiscal_year_period": Match all CRSP monthly returns within each fiscal year period (begfyr to endfyr)
#'   - "same_month": Match only the fiscal year end month
#'   - "lag_3_14": Match with 3-14 month lag after fiscal year end
#'   - "exact": Exact date match
#' @param comp_date_col Name of date column in Compustat data (default: "datadate")
#' @param crsp_date_col Name of date column in CRSP data (default: "date")
#' @param collect Logical, whether to collect at the end (default: TRUE)
#' @return Merged Compustat-CRSP data (lazy tbl if collect=FALSE, dataframe if collect=TRUE)
#'
#' @export
merge_compustat_crsp <- function(comp_data,
                                 crsp_data,
                                 link_table = NULL,
                                 con = NULL,
                                 merge_method = "fiscal_year_period",
                                 comp_date_col = "datadate",
                                 crsp_date_col = "date",
                                 collect = TRUE) {

  # Create linking table if not provided (keep it lazy)
  if (is.null(link_table)) {
    if (is.null(con)) {
      stop("Either link_table or con must be provided")
    }
    link_table <- create_compustat_crsp_link(con, collect = FALSE)
  }

  # Ensure link_table is lazy for in-database operations
  if (is.data.frame(link_table)) {
    stop("link_table must be a lazy tbl for efficient in-database operations. Use collect = FALSE in create_compustat_crsp_link()")
  }

  # IMPORTANT: All joins happen in the database BEFORE any collection
  # Step 1: Merge Compustat with linking table (in database)
  comp_linked <- comp_data %>%
    inner_join(link_table, by = "gvkey")

  # Step 2: Apply merge method and join with CRSP (all in database)
  if (merge_method == "fiscal_year_period") {
    # Match all CRSP monthly returns within each fiscal year period
    # Fiscal year period: begfyr (datadate - 11 months) to endfyr (datadate)
    comp_linked <- comp_linked %>%
      mutate(
        endfyr = !!sym(comp_date_col),  # Fiscal year end date
        begfyr = endfyr %m-% months(11)  # Fiscal year start date (11 months before end)
      )

    # Join with CRSP and filter for dates within fiscal year period
    merged <- comp_linked %>%
      inner_join(
        crsp_data,
        by = "permno"
      ) %>%
      filter(
        !!sym(crsp_date_col) >= begfyr,
        !!sym(crsp_date_col) <= endfyr
      ) %>%
      select(-begfyr, -endfyr)  # Remove helper columns

  } else if (merge_method == "same_month") {
    # Match by same month and year (all in database)
    comp_linked <- comp_linked %>%
      mutate(
        comp_month = month(!!sym(comp_date_col)),
        comp_year = year(!!sym(comp_date_col))
      )

    crsp_data <- crsp_data %>%
      mutate(
        crsp_month = month(!!sym(crsp_date_col)),
        crsp_year = year(!!sym(crsp_date_col))
      )

    # Join in database
    # When joining on comp_month = crsp_month, the columns are matched
    # After join, we need to remove the helper columns we created
    merged <- comp_linked %>%
      inner_join(
        crsp_data,
        by = c("permno" = "permno",
               "comp_month" = "crsp_month",
               "comp_year" = "crsp_year")
      )

    # Remove helper columns - only remove comp_month and comp_year
    # (crsp_month/crsp_year don't exist after join since they were matched)
    # Use a subquery approach to safely remove columns
    merged <- merged %>%
      select(-comp_month, -comp_year)

  } else if (merge_method == "lag_3_14") {
    # Match with 3-14 month lag (all in database)
    merged <- comp_linked %>%
      inner_join(
        crsp_data,
        by = "permno"
      ) %>%
      mutate(
        months_between = as.integer(
          (year(!!sym(crsp_date_col)) - year(!!sym(comp_date_col))) * 12 +
          (month(!!sym(crsp_date_col)) - month(!!sym(comp_date_col)))
        )
      ) %>%
      filter(
        months_between >= 3,
        months_between <= 14
      ) %>%
      select(-months_between)

  } else if (merge_method == "exact") {
    # Exact date match (all in database)
    merged <- comp_linked %>%
      inner_join(
        crsp_data,
        by = c("permno" = "permno",
               comp_date_col = crsp_date_col)
      )
  } else {
    stop("merge_method must be 'fiscal_year_period', 'same_month', 'lag_3_14', or 'exact'")
  }

  # Step 3: Collect ONLY after all joins are complete (all joins happened in database)
  # This ensures maximum efficiency - database does all the heavy lifting
  if (collect) {
    merged <- merged %>% collect()

    # Clean up duplicate columns with .x/.y suffixes
    # Priority: Keep funda versions for identifiers, CRSP versions for returns/prices
    merged <- cleanup_duplicate_columns(merged)
  }

  return(merged)
}

#' Clean up duplicate columns from joins (removes .x/.y suffixes)
#'
#' @param data Merged dataframe with potential duplicate columns
#' @return Dataframe with cleaned column names
#'
#' @export
cleanup_duplicate_columns <- function(data) {
  if (!is.data.frame(data)) {
    return(data)  # Only works on collected dataframes
  }

  # Columns that should come from CRSP (monthly data) - keep .y version
  crsp_priority <- c("ret", "prc", "vol", "date")

  # Columns that should come from funda (fundamentals) - keep .x version
  funda_priority <- c("tic", "cusip", "conm", "cik")

  # Process duplicates
  cols_to_remove <- character(0)

  for (col_base in c(crsp_priority, funda_priority)) {
    col_x <- paste0(col_base, ".x")
    col_y <- paste0(col_base, ".y")

    if (col_x %in% colnames(data) && col_y %in% colnames(data)) {
      # Both exist - keep the priority version
      if (col_base %in% crsp_priority) {
        # Keep .y (from CRSP), remove .x
        data[[col_base]] <- data[[col_y]]
        cols_to_remove <- c(cols_to_remove, col_x, col_y)
      } else {
        # Keep .x (from funda), remove .y
        data[[col_base]] <- data[[col_x]]
        cols_to_remove <- c(cols_to_remove, col_x, col_y)
      }
    } else if (col_x %in% colnames(data)) {
      # Only .x exists, rename it
      data[[col_base]] <- data[[col_x]]
      cols_to_remove <- c(cols_to_remove, col_x)
    } else if (col_y %in% colnames(data)) {
      # Only .y exists, rename it
      data[[col_base]] <- data[[col_y]]
      cols_to_remove <- c(cols_to_remove, col_y)
    }
  }

  # Remove the duplicate columns
  if (length(cols_to_remove) > 0) {
    data <- data %>% select(-all_of(cols_to_remove))
  }

  return(data)
}

#' Complete workflow: Get and merge Compustat and CRSP data
#'
#' @param con Database connection object
#' @param start_year Starting fiscal year for Compustat
#' @param end_year Ending fiscal year for Compustat
#' @param start_date Starting date for CRSP
#' @param end_date Ending date for CRSP
#' @param tickers Optional vector of ticker symbols
#' @param gvkeys Optional vector of GVKEYs
#' @param merge_method How to match dates (default: "same_month")
#' @param apply_comp_filters Apply Compustat filters (default: TRUE)
#' @param apply_crsp_filters Apply CRSP filters (default: TRUE)
#' @return Merged Compustat-CRSP data
#'
#' @export
get_merged_compustat_crsp <- function(con,
                                     start_year = NULL,
                                     end_year = NULL,
                                     start_date = NULL,
                                     end_date = NULL,
                                     tickers = NULL,
                                     gvkeys = NULL,
                                     merge_method = "fiscal_year_period",
                                     apply_comp_filters = TRUE,
                                     apply_crsp_filters = TRUE) {

  # Ensure connection is valid
  con <- ensure_connection(con)

  # Create linking table (keep lazy for in-database operations)
  cat("Creating linking table...\n")
  tryCatch({
    link_table <- create_compustat_crsp_link(con, tickers = tickers,
                                             gvkeys = gvkeys, collect = FALSE)
  }, error = function(e) {
    stop("Failed to create linking table: ", e$message)
  })

  # Get Compustat data (lazy)
  cat("Getting Compustat data...\n")
  tryCatch({
    comp_data <- get_compustat_funda(con,
                                     start_year = start_year,
                                     end_year = end_year,
                                     gvkeys = gvkeys,
                                     apply_filters = apply_comp_filters,
                                     collect = FALSE)
  }, error = function(e) {
    error_msg <- if (nchar(e$message) > 0) e$message else "Unknown error"
    stop("Failed to get Compustat data: ", error_msg,
         "\n  Full error details: ", paste(capture.output(print(e)), collapse = "\n"))
  })

  # Get CRSP data (lazy)
  # Note: We don't filter by permnos here - let the join handle it for efficiency
  cat("Getting CRSP data...\n")
  tryCatch({
    crsp_data <- get_crsp_msf(con,
                              start_date = start_date,
                              end_date = end_date,
                              permnos = NULL,  # Don't filter here - join will handle it
                              apply_filters = apply_crsp_filters,
                              join_msenames = apply_crsp_filters,  # Join msenames for SIC filtering
                              collect = FALSE)
  }, error = function(e) {
    stop("Failed to get CRSP data: ", e$message)
  })

  # Merge datasets - ALL JOINS HAPPEN IN DATABASE BEFORE COLLECTION
  # This is the key efficiency: database does filtering and joining, we only collect final result
  cat("Merging datasets (all joins in database)...\n")
  tryCatch({
    merged <- merge_compustat_crsp(comp_data, crsp_data, link_table,
                                   merge_method = merge_method,
                                   collect = TRUE)  # Collect ONLY after all joins complete
  }, error = function(e) {
    # Provide more context for merge errors
    error_msg <- if (nchar(e$message) > 0) e$message else "Unknown error during merge"
    stop("Failed to merge datasets: ", error_msg,
         "\n  This may be due to:\n",
         "  - No matching records between datasets\n",
         "  - Date range issues\n",
         "  - Connection timeout during merge\n",
         "  - SQL query complexity")
  })

  cat("Merging complete!\n")
  return(merged)
}


