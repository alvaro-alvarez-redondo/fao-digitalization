# script: reading script
# description: read all sheets from .xlsx files, enforce base columns,
# and return tidy data.tables with error logging

#' @title build standardized read error
#' @description formats a consistent error message for read operations across
#' the import pipeline.
#' @param context_message character scalar describing the read context.
#' @param file_path character scalar file path used for formatting.
#' @param details character scalar with low-level error details.
#' @return character vector formatted with `cli::format_error()`.
#' @importFrom checkmate check_string
#' @importFrom cli format_error
#' @importFrom fs path_file
#' @examples
#' build_read_error("failed to read", "file.xlsx", "sheet not found")
build_read_error <- function(context_message, file_path, details) {
  return(cli::format_error(c(
    "{context_message} {.file {fs::path_file(file_path)}}.",
    "x" = details
  )))
}

#' @title safely execute read operation
#' @description executes a read operation with consistent error capture and
#' standardized error formatting.
#' @param operation function with no arguments that performs the read step.
#' @param context_message character scalar describing the read context.
#' @param file_path character scalar file path used in formatted errors.
#' @return named list with `result` and `errors`.
#' @importFrom checkmate check_function check_string
#' @examples
#' safe_execute_read(
#'   operation = function() 1L,
#'   context_message = "failed to execute operation",
#'   file_path = "file.xlsx"
#' )
safe_execute_read <- function(operation, context_message, file_path) {
  assert_or_abort(checkmate::check_function(operation))
  assert_or_abort(checkmate::check_string(context_message, min.chars = 1))
  assert_or_abort(checkmate::check_string(file_path, min.chars = 1))

  return(tryCatch(
    list(result = operation(), errors = character(0)),
    error = function(condition) {
      list(
        result = NULL,
        errors = build_read_error(context_message, file_path, condition$message)
      )
    }
  ))
}


#' @title create empty read result
#' @description build a standardized read result object with empty data and
#' optional error messages.
#' @param errors character vector of error messages.
#' @return named list with `data` as empty data.table and `errors`.
#' @importFrom checkmate check_character
#' @examples
#' create_empty_read_result(character())
create_empty_read_result <- function(errors = character(0)) {
  return(list(data = data.table::data.table(), errors = errors))
}

#' @title check read result errors
#' @description return `TRUE` when a read result contains at least one error.
#' @param read_result named list returned by `safe_execute_read()`.
#' @return logical scalar.
#' @importFrom checkmate check_list
#' @examples
#' has_read_errors(list(result = NULL, errors = "x"))
has_read_errors <- function(read_result) {
  return(!is.null(read_result$errors) && length(read_result$errors) > 0)
}

#' @title assert read result contract
#' @description validate that a successful read result contains the expected
#' `data` and `errors` fields with stable types.
#' @param read_result named list returned by a read operation.
#' @return invisible `TRUE` when validation succeeds.
#' @importFrom checkmate check_list check_data_frame check_character
#' @examples
#' assert_read_result_contract(list(data = data.frame(), errors = character(0)))
assert_read_result_contract <- function(read_result) {
  assert_or_abort(checkmate::check_list(
    read_result,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_data_frame(read_result$data, min.rows = 0))
  assert_or_abort(checkmate::check_character(
    read_result$errors,
    any.missing = FALSE
  ))

  return(invisible(TRUE))
}

#' @title normalize pipeline read result
#' @description normalize the output of `safe_execute_read()` when reading a file.
#' @param read_result named list with `result` and `errors`.
#' @return named list with `data` as `data.table` and `errors` as character
#' vector.
#' @importFrom checkmate check_list
#' @examples
#' normalize_pipeline_read_result(list(result = NULL, errors = character(0)))
normalize_pipeline_read_result <- function(read_result) {
  assert_or_abort(checkmate::check_list(read_result, min.len = 1))

  if (is.null(read_result$result)) {
    return(list(
      data = create_empty_read_result()$data,
      errors = c(read_result$errors)
    ))
  }

  assert_read_result_contract(read_result$result)

  return(list(
    data = data.table::setDT(read_result$result$data),
    errors = c(read_result$errors, read_result$result$errors)
  ))
}


#' @title Compute keep-row mask for required base columns
#' @description Computes a logical vector selecting rows where at least one
#' required base column is non-missing and non-empty using a column-wise
#' Reduce to avoid materializing a full matrix copy.
#' @param read_dt data.frame or data.table containing required columns.
#' @param base_cols character vector of required base column names.
#' @return logical vector with one element per row in `read_dt`.
#' @importFrom checkmate check_data_frame check_character
#' @examples
#' compute_non_empty_base_rows(data.frame(country = c("a", "")), "country")
compute_non_empty_base_rows <- function(read_dt, base_cols) {
  ensure_data_table(read_dt)

  if (length(base_cols) == 0) {
    return(logical(nrow(read_dt)))
  }

  keep_row <- Reduce(`|`, lapply(base_cols, function(col) {
    v <- read_dt[[col]]
    !is.na(v) & trimws(v) != ""
  }))

  return(keep_row)
}

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
#' @importFrom data.table data.table as.data.table
#' @importFrom cli format_warning
#' @examples
#' config_example <- list(column_required = c("country", "year"))
#' # read_excel_sheet("1-import/10-raw_imports/example.xlsx", "sheet1", config_example)
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

  safe_read_result <- safe_execute_read(
    operation = \() {
      readxl::read_excel(
        path = file_path,
        sheet = sheet_name,
        col_names = TRUE,
        col_types = "text",
        .name_repair = "unique_quiet"
      )
    },
    context_message = paste0(
      "failed to read sheet {.val ",
      sheet_name,
      "} in file"
    ),
    file_path = file_path
  )

  if (has_read_errors(safe_read_result)) {
    return(create_empty_read_result(safe_read_result$errors))
  }

  read_dt <- data.table::setDT(safe_read_result$result)

  missing_base <- setdiff(base_cols, colnames(read_dt))

  missing_base_errors <- if (length(missing_base) > 0) {
    cli::format_warning(c(
      "sheet {.val {sheet_name}} is missing required base columns in file {.file {fs::path_file(file_path)}}.",
      "!" = paste(missing_base, collapse = ", ")
    ))
  } else {
    character(0)
  }

  if (length(missing_base) > 0) {
    read_dt[, (missing_base) := NA_character_]
  }

  keep_row <- compute_non_empty_base_rows(read_dt = read_dt, base_cols = base_cols)

  filtered_dt <- read_dt[keep_row]
  filtered_dt[, variable := sheet_name]

  return(list(data = filtered_dt, errors = missing_base_errors))
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
#' @importFrom cli format_warning
#' @examples
#' config_example <- list(column_required = c("country", "year"))
#' # read_file_sheets("1-import/10-raw_imports/example.xlsx", config_example)
read_file_sheets <- function(file_path, config) {
  assert_or_abort(checkmate::check_string(file_path, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  assert_or_abort(checkmate::check_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  ))

  safe_sheet_result <- safe_execute_read(
    operation = \() readxl::excel_sheets(file_path),
    context_message = "failed to list sheets in file",
    file_path = file_path
  )

  if (has_read_errors(safe_sheet_result)) {
    return(create_empty_read_result(safe_sheet_result$errors))
  }

  sheets <- safe_sheet_result$result

  if (length(sheets) == 0) {
    return(create_empty_read_result())
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

  sheets_list <- lapply(sheets, function(sheet_name) {
    read_excel_sheet(file_path, sheet_name, config)
  })

  combined_data <- data.table::rbindlist(
    lapply(sheets_list, `[[`, "data"),
    use.names = TRUE, fill = TRUE
  )

  combined_errors <- c(
    errors,
    unlist(lapply(sheets_list, `[[`, "errors"), use.names = FALSE)
  )

  return(list(data = combined_data, errors = combined_errors))
}

#' @title read pipeline files
#' @description iterate through discovered file metadata and read each file using
#' `read_file_sheets`. when `future` parallel backends are configured, files are
#' read in parallel using `future.apply::future_lapply()` for improved throughput
#' on large file sets (1000+ files).
#' @param file_list_dt data frame or data table with a `file_path` character
#' column. can be empty.
#' @param config named list containing `column_required` as a non-empty character
#' vector of required base column names.
#' @param progressor optional `progressr::progressor()` function used to advance a
#' shared import-pipeline progress bar. when `NULL`, no progress update is emitted
#' by this function.
#' @return named list with `read_data_list` as a list of `data.table` objects and
#' `errors` as a character vector.
#' @importFrom checkmate check_character check_data_frame check_list check_names check_function
#' @importFrom future.apply future_lapply
#' @examples
#' file_list_example <- data.frame(file_path = character())
#' config_example <- list(column_required = c("country", "year"))
#' read_pipeline_files(file_list_example, config_example)
read_pipeline_files <- function(file_list_dt, config, progressor = NULL) {
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

  if (!is.null(progressor)) {
    assert_or_abort(checkmate::check_function(progressor))
  }
  assert_or_abort(checkmate::check_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  ))

  if (nrow(file_list_dt) == 0) {
    return(list(read_data_list = list(), errors = character(0)))
  }

  file_paths <- file_list_dt$file_path

  use_parallel <- !inherits(future::plan(), "sequential") &&
    length(file_paths) > 1L

  if (use_parallel) {
    read_results <- future.apply::future_lapply(
      file_paths,
      function(file_path) {
        safe_execute_read(
          operation = \() read_file_sheets(file_path, config),
          context_message = "failed to read pipeline file",
          file_path = file_path
        )
      },
      future.seed = NULL
    )
  } else {
    read_results <- lapply(
      file_paths,
      function(file_path) {
        if (!is.null(progressor)) {
          progressor(sprintf(
            "Import Pipeline Progress: reading %s",
            fs::path_file(file_path)
          ))
        }

        safe_execute_read(
          operation = \() read_file_sheets(file_path, config),
          context_message = "failed to read pipeline file",
          file_path = file_path
        )
      }
    )
  }

  normalized_results <- lapply(read_results, normalize_pipeline_read_result)

  read_data_list <- lapply(normalized_results, function(x) x$data)
  errors_list <- lapply(normalized_results, function(x) x$errors)

  return(list(
    read_data_list = read_data_list,
    errors = unlist(errors_list, use.names = FALSE)
  ))
}
