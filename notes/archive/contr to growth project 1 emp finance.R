add_multi_drop <- function(hc, selectors, selected = NULL) {
  # first we create a random id to ensure that if used multiple times, each selector / chart combo has a unique id
  rand_id_begin <- paste(sample(letters, 3, replace = FALSE),
    sample(letters, 4, replace = FALSE),
    sample(letters, 3, replace = FALSE),
    collapse = ""
  ) %>% stringr::str_remove_all(" ")

  # next we generate the potential list options available in the chart (to ensure that we aren't passing in multiple data sets or trying to attach the data again to the web document)
  list_opts <- purrr::map(setNames(selectors, selectors), ~ {
    select_name <- .x
    hc$x$hc_opts$series %>%
      purrr::map(~ {
        purrr::map(purrr::pluck(.x, "data"), ~ {
          purrr::pluck(.x, select_name)
        }) %>%
          unlist()
      }) %>%
      unlist() %>%
      unique()
  })

  # generate the select html input elements with their corresponding options
  select_options <- list_opts %>%
    purrr::imap(~ {
      htmltools::tags$select(
        style = "display:inline-block",
        id = paste0(.y, rand_id_begin),
        purrr::map(.x, ~ {
          if ((!is.null(selected) & .x %in% selected)) {
            htmltools::tags$option(value = ., ., `selected` = TRUE)
          } else {
            htmltools::tags$option(value = ., .)
          }
        })
      )
    })

  # generate variable declaration statement in javascript using R string interpolation (comparable to es7 `this var: ${var}`)
  names(list_opts) %>%
    purrr::map_chr(~ {
      glue::glue("var select_{paste0(.x, rand_id_begin)} = document.getElementById('{paste0(.x, rand_id_begin)}');")
    }) %>%
    paste(collapse = "") -> var_declaration

  # generate shared javascript filter (each chart should be aware of multiple inputs - and share the filter between onchange events)
  names(list_opts) %>%
    purrr::map_chr(~ {
      glue::glue("obj.{.x} == select_{paste0(.x, rand_id_begin)}.value")
    }) %>%
    paste(collapse = " & ") -> filter_declaration

  # generate the onchange events to monitor each of the inputs
  names(list_opts) %>%
    purrr::map_chr(~ {
      glue::glue("select_{paste0(.x, rand_id_begin)}.onchange = updateChart;")
    }) %>%
    paste(collapse = "") -> onchg_events

  # create the javascript function for highcharts events, adding in the variable declaration, filters, and events generated from previous steps

  js_fun <- "function(){{
  var this_chart = this;
  // create a cloned data set (to avoid highcharts mutate behavior)
  const cloneData = (sample) => {{ return JSON.parse(JSON.stringify(sample));}}
  // initialize empty array
  const init_data = [];
  // loop over chart series and add to data array
  this_chart.options.series.map((series,index)=>{{
    init_data[index] = cloneData(series.data);
  }})

  // declare variables
  {var_declaration}

  // create shared updateChart function
  function updateChart(){{
      // map the series data to filter based on inputs
      init_data.map((series,index)=>{{
      new_data = series.filter(function(obj){{
        return {filter_declaration}
        }});
      // only draw if data is available post filter
      if(new_data.length>0){{
        this_chart.series[index].setData(new_data);
      }}

      }})
    // redraw chart
    this_chart.reflow();

  }}
  // add on change monitor events for dropdowns
  {onchg_events}
  // updateChart (for first run)
  updateChart();
}}"

  # add javascript function as an onload event (so we filter down the data in the first view)
  highcharter::hc_chart(hc,
    events = list(
      load = highcharter::JS(glue::glue(js_fun))
    )
  ) %>%
    htmltools::tagList(
      select_options, . # create a html fragment including the dropdowns and chart
    ) %>%
    htmltools::browsable() # create a browsable option (shows chart in rstudio or in render)
}

analysis_data <- read_csv(path.expand("~/r_projects/wrds/notes/analysis_data_norm.csv"))


analysis_data %>%
  group_by(date) %>%
  add_count() %>%
  mutate(wt = mktcap_lag / sum(mktcap_lag, na.rm = TRUE)) %>%
  mutate(vw_contr = ret * wt, ew_contr = ret * 1 / n) %>%
  group_by(date, sector, rd_quintile) %>%
  summarize(n = mean(n, na.rm = TRUE), across(contains("contr"), ~ sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  gather(key,contr,vw_contr:ew_contr) %>%
  arrange(rd_quintile,sector,date) %>%
  mutate(contr = contr*100) %>%
  select(-n) %>%
  split(.$key) %>%
  imap(~{
    hchart(.x,"column",hcaes(date,contr,group = sector)) %>%
      hc_plotOptions(series = list(stacking = "normal")) %>%
      hc_title(text = .y) %>%
      hc_tooltip(shared = TRUE,valuePostfix = "%",valueDecimals = 2) %>%
      hc_add_theme(hc_theme_darkunica()) %>%
      add_multi_drop("rd_quintile") %>%
      # div(style = "height:45%") %>%
      tagList()
  }) %>%
  fluidPage() %>%
  htmltools::browsable()


analysis_data %>%
  group_by(date) %>%
  add_count() %>%
  mutate(wt = mktcap_lag / sum(mktcap_lag, na.rm = TRUE)) %>%
  mutate(vw_contr = ret * wt, ew_contr = ret * 1 / n) %>%
  group_by(date, sector, rd_quintile) %>%
  summarize(n = mean(n, na.rm = TRUE), across(contains("contr"), ~ sum(., na.rm = TRUE)*100)) %>%
  filter(rd_quintile == "Q1") %>%
  hchart("scatter",hcaes(vw_contr,ew_contr,group = sector)) %>%
  hc_add_theme(hc_theme_darkunica())
