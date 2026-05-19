# read utilities for import pipeline

#' Build a formatted read error message
#' Formats a contextual read-error string using `cli::format_error`.
#' @param context_message Character scalar context description.
#' @param file_path Character scalar path to the file that failed.
#' @param details Character scalar error details.
#' @return Formatted error string.
#' @examples
#' build_read_error("failed to open", "data.xlsx", "file not found")
build_read_error <- function(context_message, file_path, details) {
  return(cli::format_error(c(
    "{context_message} {.file {fs::path_file(file_path)}}.",
    "x" = details
  )))
}

#' Execute a read operation safely
#' Wraps a reading operation in `tryCatch`, returning either the result with
#' zero errors or `NULL` with a formatted error message.
#' @param operation Function to execute.
#' @param context_message Character scalar context description for errors.
#' @param file_path Character scalar path to the file being read.
#' @return Named list with `result` and `errors`.
#' @examples
#' \dontrun{
#' safe_execute_read(
#'   operation = \() readxl::read_excel("data.xlsx"),
#'   context_message = "failed to read",
#'   file_path = "data.xlsx"
#' )
#' }
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

#' Create an empty read result
#' Returns a standardized empty read result with an empty `data.table` and
#' optional error messages.
#' @param errors Character vector of error messages (default: empty).
#' @return Named list with `data` (empty `data.table`) and `errors`.
#' @examples
#' create_empty_read_result()
create_empty_read_result <- function(errors = character(0)) {
  return(list(data = data.table::data.table(), errors = errors))
}

#' Check whether a read result contains errors
#' @param read_result Named list with an `errors` element.
#' @return Logical scalar.
#' @examples
#' has_read_errors(list(data = data.table::data.table(), errors = "error"))
has_read_errors <- function(read_result) {
  return(!is.null(read_result$errors) && length(read_result$errors) > 0)
}

#' Assert that a read result satisfies its contract
#' Validates that `read_result` contains a `data` data frame and an `errors`
#' character vector.
#' @param read_result Named list representing a read result.
#' @return Invisibly returns `TRUE`.
#' @examples
#' assert_read_result_contract(list(
#'   data = data.table::data.table(),
#'   errors = character(0)
#' ))
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

#' Normalize a pipeline read result
#' Coerces the `data` element to `data.table` and merges error vectors from
#' nested read results into a flat structure.
#' @param read_result Named list returned by `safe_execute_read()`.
#' @return Named list with `data` (`data.table`) and `errors` (character).
#' @examples
#' normalize_pipeline_read_result(list(
#'   result = list(data = data.frame(), errors = character(0)),
#'   errors = character(0)
#' ))
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
