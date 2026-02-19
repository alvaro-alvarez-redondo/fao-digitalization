# script: 32-export_lists.r
# description: export unique values from selected columns into excel list outputs.

#' @title get unique column values
#' @description extract, de-duplicate, and sort values from a selected column in a data frame.
#' @param df data frame containing the source records; validated with
#' `checkmate::check_data_frame(min.rows = 1)`.
#' @param col_name single character string naming an existing column in df;
#' validated with `checkmate::check_string(min.chars = 1)` and a membership
#' check against available column names.
#' @return atomic vector containing sorted unique values from the selected column.
#' @importFrom checkmate check_data_frame check_string check_choice
#' @examples
#' data_example <- data.frame(country = c("argentina", "brazil", "argentina"))
#' get_unique_column(data_example, "country")
get_unique_column <- function(df, col_name) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_string(col_name, min.chars = 1))

  df <- ensure_data_table(df)
  assert_or_abort(checkmate::check_choice(col_name, choices = colnames(df)))

  sort(unique(df[[col_name]]))
}

#' @title export single column list
#' @description export sorted unique values from one selected column to an excel file and
#' return the output path.
#' @param df data frame containing the source records; validated with
#' `checkmate::check_data_frame(min.rows = 1)`.
#' @param col_name single character string naming an existing column in df;
#' validated with `checkmate::check_string(min.chars = 1)`.
#' @param config named list with export configuration values required by
#' `generate_export_path()`, validated with
#' `checkmate::check_list(names = "named")`.
#' @param overwrite logical flag indicating whether an existing file should be
#' replaced; validated with `checkmate::check_flag()`.
#' @return character scalar containing the generated file path for the exported excel file.
#' @importFrom checkmate check_data_frame check_string check_list check_flag
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' config <- list(output_dir = tempdir())
#' data_example <- data.frame(country = c("argentina", "brazil"))
#' export_single_column_list(data_example, "country", config, overwrite = TRUE)
export_single_column_list <- function(df, col_name, config, overwrite = TRUE) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_string(col_name, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, names = "named"))
  assert_or_abort(checkmate::check_flag(overwrite))

  validate_export_import(df, col_name)

  values <- get_unique_column(df, col_name)
  path <- generate_export_path(config, col_name, type = "lists")

  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "data")
  openxlsx::writeData(wb, "data", values)
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)

  path
}

#' @title normalize sheet name
#' @description normalize column names into excel-safe worksheet names limited to thirty-one
#' characters.
#' @param col_name atomic vector of column names to normalize; validated with
#' `checkmate::check_atomic_vector(min.len = 1, any.missing = `TRUE`)`.
#' @return character vector of normalized worksheet names with empty values replaced by unknown.
#' @importFrom checkmate check_atomic_vector
#' @importFrom stringr str_sub
#' @examples
#' normalize_sheet_name(c("country name", ""))
normalize_sheet_name <- function(col_name) {
  assert_or_abort(checkmate::check_atomic_vector(
    col_name,
    min.len = 1,
    any.missing = TRUE
  ))

  sheet_name <- col_name |>
    normalize_filename() |>
    stringr::str_sub(1, 31)

  sheet_name[is.na(sheet_name) | sheet_name == ""] <- "unknown"

  sheet_name
}

#' @title export selected unique lists
#' @description export unique values from configured columns into one workbook with one
#' worksheet per column.
#' @param df data frame containing the source records; validated with
#' `checkmate::check_data_frame(min.rows = 1)`.
#' @param config named list containing export configuration, list columns, and
#' workbook name; validated with `checkmate::check_list(names = "named")`.
#' @param overwrite logical flag indicating whether an existing file should be
#' replaced; validated with `checkmate::check_flag()`.
#' @return character scalar containing the generated file path for the exported workbook.
#' @importFrom checkmate check_data_frame check_list check_flag check_character check_string
#' @importFrom purrr walk
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' config <- list(
#'   output_dir = tempdir(),
#'   export_config = list(
#'     lists_to_export = c("country"),
#'     lists_workbook_name = "fao_unique_lists_raw"
#'   )
#' )
#' data_example <- data.frame(country = c("argentina", "brazil"))
#' export_selected_unique_lists(data_example, config, overwrite = TRUE)
export_selected_unique_lists <- function(df, config, overwrite = TRUE) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_list(config, names = "named"))
  assert_or_abort(checkmate::check_flag(overwrite))
  assert_or_abort(checkmate::check_list(config$export_config, names = "named"))
  assert_or_abort(checkmate::check_character(
    config$export_config$lists_to_export,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_string(
    config$export_config$lists_workbook_name,
    min.chars = 1
  ))

  validate_export_import(df, "fao_unique_lists_raw")

  cols_to_export <- config$export_config$lists_to_export
  workbook_path <- generate_export_path(
    config,
    config$export_config$lists_workbook_name,
    type = "lists"
  )

  wb <- openxlsx::createWorkbook()

  purrr::walk(cols_to_export, function(col_name) {
    values <- get_unique_column(df, col_name)
    sheet_name <- normalize_sheet_name(col_name)
    openxlsx::addWorksheet(wb, sheet_name)
    openxlsx::writeData(wb, sheet_name, values)
  })

  openxlsx::saveWorkbook(wb, workbook_path, overwrite = overwrite)

  workbook_path
}
