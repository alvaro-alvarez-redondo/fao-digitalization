# script: 02-helpers.r
# purpose: helper functions for string normalization and export path generation.

#' @title assert checkmate validation results with cli errors
#' @description validates a checkmate `check_*` return value and aborts with a
#'   structured cli error when validation fails.
#' @param check_result character scalar or `true` returned by a
#'   `checkmate::check_*` function. expected to be `true` for valid inputs,
#'   otherwise an error message string.
#' @return invisible logical scalar `true` when validation passes. aborts with
#'   classed error via `cli::cli_abort()` when validation fails.
#' @section external dependencies:
#'   uses `checkmate` conventions for `check_*` return values and `cli` for
#'   standardized error messaging.
#' @examples
#' assert_or_abort(checkmate::check_string("ok"))
#' @importFrom cli cli_abort
assert_or_abort <- function(check_result) {
  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }

  invisible(TRUE)
}

#' @title normalize free text into lowercase ascii
#' @description converts input text to lowercase ascii, removes non-alphanumeric
#'   characters (except spaces), and squishes repeated spaces.
#' @param string atomic vector with length >= 1 and no missing values. validated
#'   with `checkmate::check_atomic(any.missing = FALSE, min.len = 1)`.
#' @return character vector with normalized lowercase ascii text and single-space
#'   token separation.
#' @section reproducibility:
#'   output is deterministic for identical input values because transformation
#'   steps are pure string operations.
#' @section external dependencies:
#'   relies on `stringr` and `stringi` for text normalization.
#' @examples
#' normalize_string("árvore! data 2024")
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all str_squish str_to_lower
#' @importFrom stringi stri_trans_general
normalize_string <- function(string) {
  assert_or_abort(checkmate::check_atomic(string, min.len = 1, any.missing = FALSE))

  string |>
    as.character() |>
    stringr::str_to_lower() |>
    stringi::stri_trans_general("latin-ascii") |>
    stringr::str_replace_all("[^a-z0-9 ]", " ") |>
    stringr::str_squish()
}

#' @title normalize file-friendly names
#' @description normalizes text and replaces spaces with underscores for safe,
#'   consistent filename stems.
#' @param filename atomic vector with length >= 1 and no missing values.
#'   validated with `checkmate::check_atomic(any.missing = FALSE, min.len = 1)`.
#' @return character vector where values are lowercase ascii and space-delimited
#'   tokens are joined with underscores.
#' @section reproducibility:
#'   delegates to `normalize_string()` to ensure deterministic normalization.
#' @examples
#' normalize_filename("food balance sheet")
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all
normalize_filename <- function(filename) {
  assert_or_abort(checkmate::check_atomic(filename, min.len = 1, any.missing = FALSE))

  filename |>
    normalize_string() |>
    stringr::str_replace_all(" ", "_")
}

#' @title extract yearbook token from parsed name parts
#' @description extracts tokens in positions 2 through 4 from a parsed filename
#'   vector and joins them with underscores when the input has enough parts.
#' @param parts character vector with length >= 1 and no missing values.
#'   validated with `checkmate::check_character(any.missing = FALSE, min.len = 1)`.
#' @return character scalar with combined yearbook tokens or `na_character_`
#'   when `parts` has fewer than 4 elements.
#' @section expected schema:
#'   expects a tokenized filename-like vector where yearbook metadata appears at
#'   indices 2:4.
#' @examples
#' extract_yearbook(c("fao", "yb", "2020", "2021", "file.xlsx"))
#' @importFrom checkmate check_character
extract_yearbook <- function(parts) {
  assert_or_abort(checkmate::check_character(parts, min.len = 1, any.missing = FALSE))

  if (length(parts) >= 4) {
    return(paste(parts[2:4], collapse = "_"))
  }

  na_character_
}

#' @title extract product token suffix from parsed name parts
#' @description extracts tokens from index 7 onward, removes extension from the
#'   final token, and joins the result with underscores.
#' @param parts character vector with length >= 1 and no missing values.
#'   validated with `checkmate::check_character(any.missing = FALSE, min.len = 1)`.
#' @return character scalar containing normalized product tokens or
#'   `na_character_` when `parts` has length <= 6.
#' @section expected schema:
#'   expects a tokenized filename-like vector where product metadata begins at
#'   position 7 and the last element may include a file extension.
#' @examples
#' extract_product(c("a", "b", "c", "d", "e", "f", "rice", "grain.xlsx"))
#' @importFrom checkmate check_character
#' @importFrom fs path_ext_remove
extract_product <- function(parts) {
  assert_or_abort(checkmate::check_character(parts, min.len = 1, any.missing = FALSE))

  if (length(parts) > 6) {
    product_parts <- parts[7:length(parts)]
    product_parts[length(product_parts)] <- fs::path_ext_remove(
      product_parts[length(product_parts)]
    )

    return(paste(product_parts, collapse = "_"))
  }

  na_character_
}

#' @title ensure data.frame input is a data.table
#' @description validates a data.frame-compatible object and returns it as a
#'   `data.table` for high-performance downstream processing.
#' @param df data.frame or `data.table` with zero or more rows. validated with
#'   `checkmate::check_data_frame(min.rows = 0)`.
#' @return `data.table::data.table` object. preserves input when already a
#'   data.table; otherwise converts via `data.table::as.data.table()`.
#' @section reproducibility:
#'   conversion is deterministic and does not mutate caller-scoped objects.
#' @examples
#' ensure_data_table(data.frame(x = 1:3))
#' @importFrom checkmate check_data_frame
#' @importFrom data.table as.data.table is.data.table
ensure_data_table <- function(df) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 0))

  if (!data.table::is.data.table(df)) {
    return(data.table::as.data.table(df))
  }

  df
}

#' @title validate export-ready import data
#' @description validates that input data has at least one row and that
#'   `base_name` is a non-empty scalar string, then returns a `data.table`.
#' @param df data.frame or data.table with at least one row. validated with
#'   `checkmate::check_data_frame(min.rows = 1)`.
#' @param base_name non-empty character scalar validated with
#'   `checkmate::check_string(min.chars = 1)`.
#' @return `data.table::data.table` with at least one row.
#' @section expected schema:
#'   `df` must be rectangular tabular data suitable for file export.
#' @examples
#' validate_export_import(data.frame(x = 1), "dataset")
#' @importFrom checkmate check_data_frame check_string
validate_export_import <- function(df, base_name) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_string(base_name, min.chars = 1))

  ensure_data_table(df)
}

#' @title build normalized export path from pipeline config
#' @description constructs a full output path for `processed` or `lists`
#'   exports by reading folder and suffix metadata from the pipeline config.
#' @param config named list containing nested keys:
#'   `paths$data$exports$processed`, `paths$data$exports$lists`,
#'   `export_config$data_suffix`, and `export_config$list_suffix`.
#'   validated with `checkmate::check_list(min.len = 1)` plus scalar checks.
#' @param base_name non-empty character scalar used as output basename before
#'   normalization and suffix append.
#' @param type character scalar, one of `"processed"` or `"lists"`.
#' @return character scalar path generated with `fs::path()`.
#' @section side effects:
#'   creates the destination directory using `fs::dir_create()` if it does not
#'   exist.
#' @section reproducibility:
#'   given an identical config and basename, the generated relative/absolute path
#'   string is deterministic.
#' @examples
#' config <- list(
#'   paths = list(data = list(exports = list(processed = "tmp", lists = "tmp"))),
#'   export_config = list(data_suffix = "_data.xlsx", list_suffix = "_list.xlsx")
#' )
#' generate_export_path(config, "food balance", "processed")
#' @importFrom checkmate check_list check_string
#' @importFrom fs dir_create path
generate_export_path <- function(
  config,
  base_name,
  type = c("processed", "lists")
) {
  assert_or_abort(checkmate::check_list(config, min.len = 1))
  assert_or_abort(checkmate::check_string(base_name, min.chars = 1))

  type <- match.arg(type)

  if (is.null(config$export_config)) {
    cli::cli_abort("`config$export_config` must be defined.")
  }

  folder <- switch(
    type,
    processed = config$paths$data$exports$processed,
    lists = config$paths$data$exports$lists
  )

  suffix <- switch(
    type,
    processed = config$export_config$data_suffix,
    lists = config$export_config$list_suffix
  )

  assert_or_abort(checkmate::check_string(folder, min.chars = 1))
  assert_or_abort(checkmate::check_string(suffix, min.chars = 1))

  fs::dir_create(folder)

  fs::path(folder, paste0(normalize_filename(base_name), suffix))
}
