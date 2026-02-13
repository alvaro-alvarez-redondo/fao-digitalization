# script: output script
# description: consolidate multiple validated long-format data.tables
# into a single data.table with controlled column order

#' @title consolidate validated data tables
#' @description consolidate multiple validated long-format tables into one
#' `data.table`, enforce configured column presence and order, and return a
#' standardized output list containing consolidated data plus warning messages.
#' @param dt_list list of data frames, data tables, or `null` elements. each
#' non-null element must be coercible to a data table.
#' @param config named list containing `column_order` as a non-empty character
#' vector defining preferred output column order.
#' @return named list with `data` as a consolidated `data.table` and `warnings`
#' as a character vector.
#' @importFrom checkmate assert_list assert_character
#' @importFrom purrr compact map
#' @importFrom data.table as.data.table data.table rbindlist setcolorder
#' @importFrom cli cli_warn
#' @examples
#' dt_list_example <- list(
#'   data.frame(product = "a", year = "2020", value = "1"),
#'   data.frame(product = "b", year = "2021", value = "2")
#' )
#' config_example <- list(column_order = c("product", "year", "value"))
#' consolidate_validated_dt(dt_list_example, config_example)
consolidate_validated_dt <- function(dt_list, config) {
  checkmate::assert_list(dt_list, any.missing = TRUE)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1
  )

  dt_list <- dt_list |>
    purrr::compact() |>
    purrr::map(data.table::as.data.table)

  if (length(dt_list) == 0) {
    cli::cli_warn("no data tables were provided for consolidation")

    return(list(
      data = data.table::data.table(),
      warnings = "no data tables were provided for consolidation"
    ))
  }

  dt_combined <- data.table::rbindlist(dt_list, use.names = TRUE, fill = TRUE)

  missing_cols <- setdiff(config$column_order, colnames(dt_combined))

  if (length(missing_cols) > 0) {
    dt_combined[, (missing_cols) := NA_character_]
  }

  desired_order <- unique(config$column_order)

  final_order <- c(
    intersect(desired_order, colnames(dt_combined)),
    setdiff(colnames(dt_combined), desired_order)
  )

  data.table::setcolorder(dt_combined, final_order)

  list(data = dt_combined, warnings = character(0))
}
