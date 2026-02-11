# ============================================================
# Script:  02-helpers.R
# Purpose: General helper functions for the pipeline, including
#          string normalization and exports subpipeline helpers.
# ============================================================

# ------------------------------
# Function. Normalize strings
# ------------------------------
normalize_string <- function(string) {
  string |>
    as.character() |>
    stringr::str_to_lower() |>
    stringi::stri_trans_general("Latin-ASCII") |>
    stringr::str_replace_all("[^a-z0-9 ]", " ") |>
    stringr::str_squish()
}

# ------------------------------
# Function. Normalize filenames
# ------------------------------
normalize_filename <- function(filename) {
  filename |>
    normalize_string() |>
    stringr::str_replace_all(" ", "_")
}

# ------------------------------
# Function. Extract yearbook from name parts
# ------------------------------
extract_yearbook <- function(parts) {
  dplyr::if_else(
    length(parts) >= 4,
    paste(parts[2:4], collapse = "_"),
    NA_character_
  )
}

# ------------------------------
# Function. Extract product from name parts
# ------------------------------
extract_product <- function(parts) {
  dplyr::if_else(
    length(parts) > 6,
    {
      prod_parts <- parts[7:length(parts)]
      prod_parts[length(prod_parts)] <- fs::path_ext_remove(prod_parts[length(prod_parts)])
      paste(prod_parts, collapse = "_")
    },
    NA_character_
  )
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
# Function. Generate full export paths
# ------------------------------
generate_export_path <- function(
  config,
  base_name,
  type = c("processed", "lists")
) {
  # Validate arguments
  type <- match.arg(type)
  stopifnot(!is.null(config$export_config))
  checkmate::assert_string(base_name)

  # Determine folder and suffix based on type
  folder <- switch(
    type,
    processed = config$paths$data$exports$processed,
    lists = config$paths$data$exports$lists
  )

  suffix <- switch(
    type,
    processed = config$export_config$excel_suffix,
    lists = config$export_config$list_suffix
  )

  fs::dir_create(folder)

  fs::path(folder, paste0(normalize_filename(base_name), suffix))
}

# ------------------------------
# Function. Safe data.table coercion
# ------------------------------
as_data_table_safe <- function(df) {
  if (!data.table::is.data.table(df)) data.table::as.data.table(df) else df
}

# ------------------------------
# End of script
# ------------------------------
