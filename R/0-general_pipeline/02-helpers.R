# ============================================================
# Script:  02-helpers.R
# Purpose: General helper functions for the pipeline, including
#          string normalization and exports subpipeline helpers.
# ============================================================

# ------------------------------
# Function. Normalize strings
# ------------------------------
normalize_string <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringi::stri_trans_general("Latin-ASCII") |>
    stringr::str_replace_all("[^a-z0-9 ]", " ") |>
    stringr::str_squish()
}

# ------------------------------
# Function. Normalize strings for filenames
# ------------------------------
normalize_filename <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringi::stri_trans_general("Latin-ASCII") |>
    stringr::str_replace_all("[^a-z0-9_]", "_") |>
    stringr::str_squish() |>
    stringr::str_replace_all("_+", "_")
}

# ------------------------------
# Function. Ensure import is data.table
# ------------------------------
ensure_data_table <- function(df) {
  if (!data.table::is.data.table(df)) {
    df <- data.table::as.data.table(df)
  }
  df
}

# ------------------------------
# Function. Validate data before export
# ------------------------------
validate_export_import <- function(df, base_name) {
  checkmate::assert_data_frame(df, min.rows = 1)
  checkmate::assert_string(base_name)
  ensure_data_table(df)
}

# ------------------------------
# Function. Generate full Excel export path
# ------------------------------
generate_excel_path <- function(config, base_name) {
  stopifnot(!is.null(config$export_config))
  fs::path(
    config$paths$data$exports$processed,
    paste0(normalize_filename(base_name), config$export_config$excel_suffix)
  )
}

# ------------------------------
# Function. Generate unique columns list path
# ------------------------------
generate_list_path <- function(config, base_name) {
  stopifnot(!is.null(config$export_config))
  fs::path(
    config$paths$data$exports$lists,
    paste0(normalize_filename(base_name), config$export_config$list_suffix)
  )
}

# ------------------------------
# Function. Safe data.table coercion
# ------------------------------
as_data_table_safe <- function(df) {
  if (!data.table::is.data.table(df)) data.table::as.data.table(df) else df
}

# ------------------------------
# Function. Dynamic export path generator
# ------------------------------
export_path <- function(config, base_name, type = c("full", "list")) {
  type <- match.arg(type)
  suffix <- if (type == "full") {
    config$export_config$excel_suffix
  } else {
    config$export_config$list_suffix
  }
  fs::path(
    config$paths$data$exports[[ifelse(type == "full", "processed", "lists")]],
    paste0(normalize_filename(base_name), suffix)
  )
}

# ------------------------------
# End of script
# ------------------------------
