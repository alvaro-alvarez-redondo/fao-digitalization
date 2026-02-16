# script: output script
# description: consolidate multiple validated long-format data.tables
# into a single data.table with controlled column order

#' @title consolidate validated data tables
#' @description consolidate multiple validated long-format tables into one
#' `data.table`, enforce configured schema coverage and target analytical column
#' order using `dplyr::relocate()`, and return standardized output with
#' warning messages.
#' @param dt_list list of data frames, data tables, or `null` elements. each
#' non-null element must be coercible to a data table.
#' @param config named list containing `column_order` as a non-empty character
#' vector defining preferred output column order.
#' @return named list with `data` as a consolidated `data.table` and `warnings`
#' as a character vector.
#' @importFrom checkmate assert_list assert_character assert_subset
#' @importFrom purrr compact map
#' @importFrom data.table as.data.table data.table rbindlist
#' @importFrom dplyr relocate any_of
#' @importFrom cli cli_warn cli_abort
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

  checkmate::assert_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1,
    unique = TRUE
  )
  checkmate::assert_subset(target_schema, choices = config$column_order)

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

  original_column_names <- colnames(dt_combined)

  dt_reordered <- dt_combined |>
    dplyr::relocate(dplyr::any_of(config$column_order)) |>
    data.table::as.data.table()

  reordered_column_names <- colnames(dt_reordered)

  if (!setequal(original_column_names, reordered_column_names)) {
    cli::cli_abort("column reorder changed the schema unexpectedly")
  }

  list(data = dt_reordered, warnings = character(0))
}
