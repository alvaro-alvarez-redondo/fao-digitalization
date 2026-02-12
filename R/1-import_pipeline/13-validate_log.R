# ============================================================
# Script:  13-validate_log.R
# Purpose: Validate mandatory fields, detect duplicates,
#          and collect errors/warnings in a tidy, functional style.
# ============================================================

# ------------------------------
# Function. Validate mandatory fields (vectorized)
# ------------------------------
validate_mandatory_fields_dt <- function(dt, config) {
  mandatory_cols <- config$column_required

  missing_mandatory_cols <- setdiff(mandatory_cols, colnames(dt))
  if (length(missing_mandatory_cols) > 0) {
    dt[, (missing_mandatory_cols) := NA_character_]
  }

  if (!("document" %in% colnames(dt))) {
    dt[, document := "unknown_document"]
  }

  missing_long <- dt[, row_id := .I][] |>
    tidyr::pivot_longer(
      cols = tidyselect::all_of(mandatory_cols),
      names_to = "column_name",
      values_to = "column_value"
    ) |>
    dplyr::filter(is.na(column_value) | column_value == "") |>
    dplyr::mutate(
      error_message = paste0(
        "missing mandatory value in document '",
        document,
        "', row_id '",
        row_id,
        "', column '",
        column_name,
        "'"
      )
    )

  errors <- missing_long$error_message |> unique()
  dt[, row_id := NULL]

  list(errors = errors, data = dt)
}

# ------------------------------
# Function. Detect duplicates
# ------------------------------
detect_duplicates_dt <- function(dt) {
  dup_counts <- dt[, .(duplicate_count = .N), by = .(product, variable, year, value, document)]
  dup_rows <- dup_counts[duplicate_count > 1]

  errors <- character(0)
  if (nrow(dup_rows) > 0) {
    errors <- paste0(
      "Duplicate entries detected for product '",
      dup_rows$product,
      "', variable '",
      dup_rows$variable,
      "', year '",
      dup_rows$year,
      "', value '",
      dup_rows$value,
      "', duplicate_count '",
      dup_rows$duplicate_count,
      "' in document '",
      dup_rows$document,
      "'"
    )
  }

  list(errors = errors, data = dt)
}

# ------------------------------
# Function. Full validation pipeline
# ------------------------------
validate_long_dt <- function(long_dt, config) {
  dt <- data.table::as.data.table(long_dt)
  errors <- character(0)

  # 1. Validate mandatory fields
  mandatory_result <- validate_mandatory_fields_dt(dt, config)
  errors <- c(errors, mandatory_result$errors)

  # 2. Detect duplicates
  duplicate_result <- detect_duplicates_dt(dt)
  errors <- c(errors, duplicate_result$errors)

  list(
    data = dt,
    errors = errors
  )
}

# ------------------------------
# End of script
# ------------------------------
