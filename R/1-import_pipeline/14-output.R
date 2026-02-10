# ============================================================
# Script:  14-output.R
# Purpose: Consolidate multiple validated long-format data.tables
#          into a single data.table with controlled column order
# ============================================================

# ------------------------------
# Function. Consolidate multiple validated data.tables
# ------------------------------
consolidate_validated_dt <- function(dt_list, column_order) {
  dt_list <- dt_list |>
    purrr::compact() |>
    purrr::map(data.table::as.data.table)
  if (length(dt_list) == 0) {
    return(list(
      data = data.table::data.table(),
      warnings = "No data.tables to consolidate."
    ))
  }

  dt_combined <- data.table::rbindlist(dt_list, use.names = TRUE, fill = TRUE)

  # Ensure all required columns exist
  missing_cols <- setdiff(column_order, colnames(dt_combined))
  if (length(missing_cols) > 0) {
    dt_combined[, (missing_cols) := NA_character_]
  }

  # Reorder columns
  data.table::setcolorder(dt_combined, column_order)

  list(data = dt_combined, warnings = character(0))
}

# ------------------------------
# End of script
# ------------------------------
