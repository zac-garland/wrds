
#' Connect to WRDS PostgreSQL using credentials from Renviron.
#'
#' Set \code{WRDS_USERNAME} and \code{WRDS_PASSWORD} in your \code{.Renviron}
#' (e.g. via \code{usethis::edit_r_environ()}). If \code{WRDS_PASSWORD} is not set,
#' RPostgres may prompt or use \code{.pgpass}.
#'
#' @return A connection object from \code{RPostgres::dbConnect(Postgres(), ...)}.
#' @export
connect_wharton <- function() {
  library(RPostgres)
  user <- Sys.getenv("WRDS_USERNAME", unset = "zkg232")
  pass <- Sys.getenv("WRDS_PASSWORD", unset = "")
  args <- list(
    host = "wrds-pgdata.wharton.upenn.edu",
    port = 9737L,
    dbname = "wrds",
    sslmode = "require",
    user = user
  )
  if (nzchar(pass)) args[["password"]] <- pass
  do.call(dbConnect, c(list(Postgres()), args))
}

check_wharton_connection <- function(){
  if (exists("con", where = 1)) {  # 1 refers to global environment
    connection <- con
  } else {
    connection <- connect_wharton()
    con <<- connection
  }
  con
}

#' @export
get_wrds_tables <- function(){
  con = check_wharton_connection()
  tbl(con,dbplyr::in_schema("information_schema","tables")) %>%
    filter(table_type == 'VIEW' | table_type == 'FOREIGN TABLE') %>%
    distinct(table_schema,table_name) %>%
    collect()
}



wharton_table <- function(db,name){
  con <- check_wharton_connection()

  safe_tbl <- safely(tbl)

  res <- safe_tbl(con,dbplyr::in_schema(db,name))

  if(length(res$error)>0){
    res$error = res$error$parent$message %>% str_trim()
  }
  res

}



