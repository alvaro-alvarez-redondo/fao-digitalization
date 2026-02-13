# script: helpers script
# description: provides helper functions for validation, string normalization,
# export path generation, and data.table coercion used across the pipeline

#' @title assert checkmate validation results with cli errors
#' @description validates the output of a `checkmate::check_*` call, requiring a
#' `true` logical scalar or a non-empty error string. when validation fails, the
#' function aborts with a structured cli error message.
#' @param check_result logical true scalar or non-empty character scalar returned
#' by a `checkmate::check_*` function.
#' @return invisible logical true scalar when validation succeeds.
#' @importFrom checkmate assert check_string check_true
#' @importFrom cli cli_abort
#' @examples
#' assert_or_abort(checkmate::check_string("ok"))
assert_or_abort <- function(check_result) {
  checkmate::assert(
    checkmate::check_true(check_result),
    checkmate::check_string(check_result, min.chars = 1)
  )

  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }

  invisible(TRUE)
}

#' @title normalize free text into lowercase ascii
#' @description converts input text to lowercase ascii, removes non-alphanumeric
#' characters except spaces, and squishes repeated spaces to one separator.
#' @param string atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector with normalized lowercase ascii text.
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all str_squish str_to_lower
#' @importFrom stringi stri_trans_general
#' @examples
#' normalize_string("árvore! data 2024")
normalize_string <- function(string) {
  assert_or_abort(checkmate::check_atomic(
    string,
    min.len = 1,
    any.missing = TRUE
  ))

  string |>
    as.character() |>
    stringr::str_to_lower() |>
    stringi::stri_trans_general("latin-ascii") |>
    stringr::str_replace_all("[^a-z0-9 ]", " ") |>
    stringr::str_squish()
}

#' @title normalize file-friendly names
#' @description normalizes text and replaces spaces with underscores for
#' deterministic filename stems. missing and empty outputs are replaced by
#' `"unknown"`.
#' @param filename atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector containing lowercase ascii filename stems.
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all
#' @examples
#' normalize_filename("food balance sheet")
normalize_filename <- function(filename) {
  assert_or_abort(checkmate::check_atomic(
    filename,
    min.len = 1,
    any.missing = TRUE
  ))

  normalized_filename <- filename |>
    normalize_string() |>
    stringr::str_replace_all(" ", "_")

  normalized_filename[
    is.na(normalized_filename) | normalized_filename == ""
  ] <- "unknown"

  normalized_filename
}

#' @title extract yearbook token from parsed name parts
#' @description extracts tokens in positions two through four from a parsed
#' filename token vector and joins them with underscores.
#' @param parts character vector with no missing values and length greater than
#' or equal to one. validated with
#' `checkmate::check_character(min.len = 1, any.missing = false)`.
#' @return character scalar with combined yearbook tokens, or `NA_character_`
#' when the input has fewer than four elements.
#' @importFrom checkmate check_character
#' @examples
#' extract_yearbook(c("fao", "yb", "2020", "2021", "file.xlsx"))
extract_yearbook <- function(parts) {
  assert_or_abort(checkmate::check_character(
    parts,
    min.len = 1,
    any.missing = FALSE
  ))

  if (length(parts) >= 4) {
    return(paste(parts[2:4], collapse = "_"))
  }

  NA_character_
}

#' @title extract product token suffix from parsed name parts
#' @description extracts tokens from index seven onward, removes the file
#' extension from the final token, and joins the result with underscores.
#' @param parts character vector with no missing values and length greater than
#' or equal to one. validated with
#' `checkmate::check_character(min.len = 1, any.missing = false)`.
#' @return character scalar with product tokens, or `NA_character_` when the
#' input has fewer than seven elements.
#' @importFrom checkmate check_character
#' @importFrom fs path_ext_remove
#' @examples
#' extract_product(c("a", "b", "c", "d", "e", "f", "rice", "grain.xlsx"))
extract_product <- function(parts) {
  assert_or_abort(checkmate::check_character(
    parts,
    min.len = 1,
    any.missing = FALSE
  ))

  if (length(parts) > 6) {
    product_parts <- parts[7:length(parts)]
    product_parts[length(product_parts)] <- fs::path_ext_remove(
      product_parts[length(product_parts)]
    )

    return(paste(product_parts, collapse = "_"))
  }

  NA_character_
}

#' @title ensure data.frame input is a data.table
#' @description validates a data.frame-compatible input and returns a
#' `data.table`, preserving existing `data.table` inputs unchanged.
#' @param df data.frame or data.table with zero or more rows. validated with
#' `checkmate::check_data_frame(min.rows = 0)`.
#' @return data.table object.
#' @importFrom checkmate check_data_frame
#' @importFrom data.table as.data.table is.data.table
#' @examples
#' ensure_data_table(data.frame(x = 1:3))
ensure_data_table <- function(df) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 0))

  if (!data.table::is.data.table(df)) {
    return(data.table::as.data.table(df))
  }

  df
}

#' @title validate export-ready import data
#' @description validates export inputs and returns a data.table for stable
#' downstream export operations.
#' @param df data.frame or data.table with at least one row. validated with
#' `checkmate::check_data_frame(min.rows = 1)`.
#' @param base_name non-empty character scalar. validated with
#' `checkmate::check_string(min.chars = 1)`.
#' @return data.table with at least one row.
#' @importFrom checkmate check_data_frame check_string
#' @examples
#' validate_export_import(data.frame(x = 1), "dataset")
validate_export_import <- function(df, base_name) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_string(base_name, min.chars = 1))

  ensure_data_table(df)
}

#' @title build normalized export path from pipeline config
#' @description constructs an output path for `processed` or `lists` exports
#' using folder and suffix metadata from the pipeline config.
#' @param config named list with non-empty structure. validated with
#' `checkmate::check_list(min.len = 1)`. must contain
#' `paths$data$exports$processed`, `paths$data$exports$lists`,
#' `export_config$data_suffix`, and `export_config$list_suffix` as non-empty
#' character scalars.
#' @param base_name non-empty character scalar used as output basename before
#' normalization and suffix append. validated with
#' `checkmate::check_string(min.chars = 1)`.
#' @param type character scalar. one of `"processed"` or `"lists"`.
#' @return character scalar path generated with `fs::path()`.
#' @importFrom checkmate check_list check_string
#' @importFrom fs dir_create path
#' @importFrom cli cli_abort
#' @examples
#' config <- list(
#'   paths = list(data = list(exports = list(processed = "tmp", lists = "tmp"))),
#'   export_config = list(data_suffix = "_data.xlsx", list_suffix = "_list.xlsx")
#' )
#' generate_export_path(config, "food balance", "processed")
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
