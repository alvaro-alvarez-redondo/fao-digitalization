# script: reading script
# description: read all sheets from .xlsx files, enforce base columns,
# and return tidy data.tables with error logging

#' @title read excel sheet
#' @description read one excel sheet as text columns, validate required inputs,
#' enforce required base columns, and return a standardized list containing a
#' `data.table` plus any validation or read errors.
#' @param file_path character scalar path to an existing xlsx file.
#' @param sheet_name character scalar with sheet name to read.
#' @param config named list containing `column_required` as a non-empty character
#' vector of required base column names.
#' @return named list with two elements: `data` as a `data.table` and `errors` as
#' a character vector. `data` includes `variable` with the sheet name and is
#' filtered to rows where at least one required base column is non-empty.
#' @importFrom checkmate check_character check_list check_string
#' @importFrom readxl read_excel
#' @importFrom fs path_file
#' @importFrom data.table data.table as.data.table
#' @importFrom dplyr filter if_any mutate
#' @importFrom tidyselect all_of
#' @importFrom cli format_error
#' @examples
#' config_example <- list(column_required = c("country", "year"))
#' # read_excel_sheet("imports/raw/example.xlsx", "sheet1", config_example)
read_excel_sheet <- function(file_path, sheet_name, config) {
  assert_or_abort(checkmate::check_string(file_path, min.chars = 1))
  assert_or_abort(checkmate::check_string(sheet_name, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  base_cols <- config$column_required
  assert_or_abort(checkmate::check_character(
    base_cols,
    any.missing = FALSE,
    min.len = 1
  ))

  safe_read_result <- tryCatch(
    readxl::read_excel(
      path = file_path,
      sheet = sheet_name,
      col_names = TRUE,
      col_types = "text",
      .name_repair = "unique_quiet"
    ),
    error = function(condition) {
      structure(
        list(error_message = condition$message),
        class = "read_error"
      )
    }
  )

  if (inherits(safe_read_result, "read_error")) {
    return(list(
      data = data.table::data.table(),
      errors = cli::format_error(c(
        "failed to read sheet {.val {sheet_name}} in file {.file {fs::path_file(file_path)}}.",
        "x" = safe_read_result$error_message
      ))
    ))
  }

  missing_base <- setdiff(base_cols, colnames(safe_read_result))

  missing_base_errors <- if (length(missing_base) > 0) {
    cli::format_error(c(
      "sheet {.val {sheet_name}} is missing required base columns in file {.file {fs::path_file(file_path)}}.",
      "i" = paste(missing_base, collapse = ", ")
    ))
  } else {
    character(0)
  }

  safe_read_result[missing_base] <- NA_character_

  list(
    data = safe_read_result |>
      dplyr::filter(
        dplyr::if_any(
          tidyselect::all_of(base_cols),
          \(column_value) !is.na(column_value) & column_value != ""
        )
      ) |>
      dplyr::mutate(variable = sheet_name) |>
      data.table::as.data.table(),
    errors = missing_base_errors
  )
}

#' @title read file sheets
#' @description list all sheets in an excel file, detect non-ascii sheet names,
#' read each sheet through `read_excel_sheet`, and combine data and errors into a
#' single standardized result.
#' @param file_path character scalar path to an xlsx file.
#' @param config named list containing `column_required` as a non-empty character
#' vector of required base column names.
#' @return named list with `data` as a combined `data.table` and `errors` as a
#' character vector containing read and validation issues.
#' @importFrom checkmate check_character check_list check_string
#' @importFrom readxl excel_sheets
#' @importFrom fs path_file
#' @importFrom data.table data.table rbindlist
#' @importFrom stringi stri_enc_isascii
#' @importFrom purrr map
#' @importFrom cli format_error format_warning
#' @examples
#' config_example <- list(column_required = c("country", "year"))
#' # read_file_sheets("imports/raw/example.xlsx", config_example)
read_file_sheets <- function(file_path, config) {
  assert_or_abort(checkmate::check_string(file_path, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))
  assert_or_abort(checkmate::check_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  ))

  sheets <- tryCatch(
    readxl::excel_sheets(file_path),
    error = function(condition) {
      structure(
        list(error_message = condition$message),
        class = "read_error"
      )
    }
  )

  if (inherits(sheets, "read_error")) {
    return(list(
      data = data.table::data.table(),
      errors = cli::format_error(c(
        "failed to list sheets in file {.file {fs::path_file(file_path)}}.",
        "x" = sheets$error_message
      ))
    ))
  }

  if (length(sheets) == 0) {
    return(list(data = data.table::data.table(), errors = character(0)))
  }

  non_ascii <- sheets[!stringi::stri_enc_isascii(sheets)]

  errors <- if (length(non_ascii) > 0) {
    cli::format_warning(c(
      "found non-ascii sheet names in file {.file {fs::path_file(file_path)}}.",
      "!" = paste(non_ascii, collapse = ", ")
    ))
  } else {
    character(0)
  }

  sheets_list <- purrr::map(sheets, \(sheet_name) {
    read_excel_sheet(file_path, sheet_name, config)
  })

  combined_data <- sheets_list |>
    purrr::map("data") |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)

  combined_errors <- c(
    errors,
    sheets_list |> purrr::map("errors") |> unlist(use.names = FALSE)
  )

  list(data = combined_data, errors = combined_errors)
}

#' @title read pipeline files
#' @description iterate through discovered file metadata and read each file using
#' `read_file_sheets`, returning a list of per-file data tables and a flattened
#' error vector.
#' @param file_list_dt data frame or data table with a `file_path` character
#' column. can be empty.
#' @param config named list containing `column_required` as a non-empty character
#' vector of required base column names.
#' @return named list with `read_data_list` as a list of `data.table` objects and
#' `errors` as a character vector.
#' @importFrom checkmate check_character check_data_frame check_list check_names
#' @importFrom purrr map transpose
#' @examples
#' file_list_example <- data.frame(file_path = character())
#' config_example <- list(column_required = c("country", "year"))
#' read_pipeline_files(file_list_example, config_example)
read_pipeline_files <- function(file_list_dt, config) {
  assert_or_abort(checkmate::check_data_frame(file_list_dt, min.cols = 1))
  assert_or_abort(checkmate::check_names(
    names(file_list_dt),
    must.include = "file_path",
    what = "names(file_list_dt)"
  ))
  assert_or_abort(checkmate::check_character(
    file_list_dt$file_path,
    any.missing = FALSE,
    null.ok = TRUE
  ))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))
  assert_or_abort(checkmate::check_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  ))

  if (nrow(file_list_dt) == 0) {
    return(list(read_data_list = list(), errors = character(0)))
  }

  read_results <- purrr::map(
    file_list_dt$file_path,
    \(file_path) read_file_sheets(file_path, config)
  )

  parsed_results <- purrr::transpose(read_results)

  list(
    read_data_list = parsed_results$data,
    errors = parsed_results$errors |> unlist(use.names = FALSE)
  )
}
