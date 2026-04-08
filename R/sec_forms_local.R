#' SEC forms locally: query WRDS and read full text from SEC.gov
#'
#' WRDS cloud EDGAR URLs require WRDS login; form full text is available
#' from the public SEC.gov API. Use \code{WRDS_USERNAME} and \code{WRDS_PASSWORD}
#' in \code{.Renviron} for the WRDS connection.
#'
#' SEC requires a User-Agent header; set \code{SEC_EDGAR_USER_AGENT} in
#' \code{.Renviron} (e.g. \code{Your Name <your.email@domain.com>}) or pass
#' \code{user_agent} to \code{read_sec_form_text}.

#' Get SEC form list for a company from WRDS \code{wrdssec.wrds_forms}.
#'
#' @param con WRDS connection from \code{connect_wharton()}.
#' @param company_name Company name as in \code{coname} (e.g. \code{"MASTERCARD INC"}).
#' @return A tibble with at least \code{coname}, \code{fname}, and any other columns.
#' @export
get_sec_forms <- function(con, company_name) {
  forms <- wharton_table("wrdssec", "wrds_forms")
  if (!is.null(forms$error)) stop("WRDS table wrdssec.wrds_forms: ", forms$error)
  forms$result %>%
    filter(coname == !!company_name) %>%
    collect()
}

#' Parse CIK and accession from WRDS \code{fname}.
#'
#' \code{fname} is typically like \code{0001141391-16-000216.txt}. The numeric
#' part before the first hyphen (zero-padded) is the CIK; the full stem is the
#' accession.
#'
#' @param fname Character vector of filenames (e.g. \code{0001141391-16-000216.txt}).
#' @return A tibble with columns \code{cik} (character, no leading zeros) and
#'   \code{accession} (e.g. \code{0001141391-16-000216}).
#' @export
fname_to_cik_accession <- function(fname) {
  stem <- sub("\\.txt$", "", fname, ignore.case = TRUE)
  # First numeric block (zero-padded CIK)
  cik_str <- sub("-.*", "", stem)
  cik <- as.character(as.integer(cik_str))
  tibble(cik = cik, accession = stem)
}

#' Fetch full text of an SEC filing from SEC.gov.
#'
#' Uses \code{https://www.sec.gov/Archives/edgar/data/{cik}/{accession}.txt}.
#' SEC requires a User-Agent header; set \code{SEC_EDGAR_USER_AGENT} in
#' \code{.Renviron} or pass \code{user_agent}.
#'
#' @param accession Accession number (e.g. \code{0001141391-16-000216}).
#' @param cik CIK as character (e.g. \code{1141391}). If \code{NULL}, derived from
#'   \code{accession} (leading digits before first hyphen).
#' @param user_agent User-Agent string for SEC (default: \code{Sys.getenv("SEC_EDGAR_USER_AGENT")}).
#'   Use e.g. \code{"Your Name <your.email@domain.com>"}.
#' @param save_path If set, save response body to this file path.
#' @return Character vector of the filing text (one element per line), or
#'   invisibly \code{save_path} if \code{save_path} is set.
#' @export
read_sec_form_text <- function(accession, cik = NULL, user_agent = NULL, save_path = NULL) {
  if (is.null(cik)) {
    cik_str <- sub("-.*", "", accession)
    cik <- as.character(as.integer(cik_str))
  }
  url <- sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s.txt", cik, accession)
  ua <- if (!is.null(user_agent) && nzchar(user_agent)) {
    user_agent
  } else {
    Sys.getenv("SEC_EDGAR_USER_AGENT", "")
  }
  if (!nzchar(ua)) {
    warning("SEC recommends a User-Agent. Set SEC_EDGAR_USER_AGENT in .Renviron or pass user_agent.")
    ua <- "R wrds package (local research)"
  }
  r <- httr::GET(url, httr::add_headers("User-Agent" = ua))
  httr::stop_for_status(r)
  text <- httr::content(r, as = "text", encoding = "UTF-8")
  if (!is.null(save_path) && nzchar(save_path)) {
    writeLines(text, save_path)
    return(invisible(save_path))
  }
  strsplit(text, "\n", fixed = TRUE)[[1]]
}

# ---- Example (run locally) ----
# con <- connect_wharton()   # uses WRDS_USERNAME / WRDS_PASSWORD from .Renviron
# ma_forms <- get_sec_forms(con, "MASTERCARD INC")
# View(ma_forms)
# last_fname <- ma_forms %>% tail(1) %>% pull(fname)
# ids <- fname_to_cik_accession(last_fname)
# text <- read_sec_form_text(ids$accession, ids$cik)
# # Or set in .Renviron: SEC_EDGAR_USER_AGENT="Your Name <your.email@domain.com>"
