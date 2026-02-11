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

  # Vectorized check: TRUE si NA o ""
  missing_mask <- dt[,
    lapply(.SD, function(x) is.na(x) | x == ""),
    .SDcols = mandatory_cols
  ]
  rows_with_missing <- which(Reduce(`|`, missing_mask))

  errors <- character(0)
  if (length(rows_with_missing) > 0) {
    missing_info <- dt[rows_with_missing, .(document)]
    errors <- paste0(
      "Missing mandatory columns in document '",
      missing_info$document,
      "'"
    )
  }

  list(errors = errors, data = dt)
}

# ------------------------------
# Function. Detect duplicates
# ------------------------------
detect_duplicates_dt <- function(dt) {
  dup_counts <- dt[, .N, by = .(product, variable, year, value, document)]
  dup_rows <- dup_counts[N > 1]

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
