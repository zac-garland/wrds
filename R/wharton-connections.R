
#' @export
connect_wharton <- function(){
  library(RPostgres)
  dbConnect(Postgres(),
            host='wrds-pgdata.wharton.upenn.edu',
            port=9737,
            dbname='wrds',
            sslmode='require',
            user='zkg232')
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



