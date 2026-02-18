# script: output script
# description: consolidate multiple validated long-format data.tables
# into a single data.table with controlled column order

#' @title validate output column order
#' @description validate that `config$column_order` exists, is unique, and
#'
#' covers the target export schema used by the import pipeline.
#'
#' @param config named list containing `column_order`.
#' @return character vector with validated output column order.
#' @importFrom checkmate check_list check_character check_subset
#' @importFrom cli cli_abort
#' @examples
#' validate_output_column_order(list(column_order = c("product", "year", "value")))
validate_output_column_order <- function(config) {
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  if (is.null(config$column_order)) {
    cli::cli_abort("`config$column_order` must be defined.")
  }

  assert_or_abort(checkmate::check_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1,
    unique = TRUE
  ))

  target_schema <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "value",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

  assert_or_abort(checkmate::check_subset(
    target_schema,
    choices = config$column_order
  ))

  config$column_order
}

#' @title consolidate validated data tables
#' @description consolidate multiple validated long-format tables into one
#'
#' `data.table`, enforce configured schema coverage, and return standardized
#' output with warning messages.
#'
#' @param dt_list list of data frames, data tables, or `null` elements. each
#' non-null element must be coercible to a data table.
#' @param config named list containing `column_order` as a non-empty character
#' vector defining preferred output column order.
#' @return named list with `data` as a consolidated `data.table` and `warnings`
#' as a character vector.
#' @importFrom checkmate check_list
#' @importFrom purrr compact map
#' @importFrom data.table data.table rbindlist setcolorder
#' @importFrom cli cli_warn
#' @examples
#' dt_list_example <- list(
#'   data.frame(product = "a", year = "2020", value = "1"),
#'   data.frame(product = "b", year = "2021", value = "2")
#' )
#' config_example <- list(column_order = c("product", "year", "value"))
#' consolidate_audited_dt(dt_list_example, config_example)
consolidate_audited_dt <- function(dt_list, config) {
  assert_or_abort(checkmate::check_list(dt_list, any.missing = TRUE))

  column_order <- validate_output_column_order(config)

  dt_items <- dt_list |>
    purrr::compact() |>
    purrr::map(coerce_to_data_table)

  if (length(dt_items) == 0) {
    warning_message <- "no data tables were provided for consolidation"
    cli::cli_warn(warning_message)

    return(list(
      data = data.table::data.table(),
      warnings = warning_message
    ))
  }

  dt_combined <- data.table::rbindlist(dt_items, use.names = TRUE, fill = TRUE)

  missing_cols <- setdiff(column_order, colnames(dt_combined))

  if (length(missing_cols) > 0) {
    dt_combined[, (missing_cols) := NA_character_]
  }

  extra_cols <- setdiff(colnames(dt_combined), column_order)
  data.table::setcolorder(dt_combined, c(column_order, extra_cols))

  list(data = dt_combined, warnings = character(0))
}
