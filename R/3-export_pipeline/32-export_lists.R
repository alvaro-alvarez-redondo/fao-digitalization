# ============================================================
# Script:  32-export_lists.R
# Purpose: Export unique values for specified columns as
#          individual Excel files in 'lists' folder.
# ============================================================

# ------------------------------
# Function. Extract unique values for a single column
# ------------------------------
get_unique_column <- function(df, col_name) {
  checkmate::assert_data_frame(df)
  checkmate::assert_string(col_name)
  df <- ensure_data_table(df)
  checkmate::assert_true(col_name %in% colnames(df), .var.name = "col_name")
  sort(unique(df[[col_name]]))
}

# ------------------------------
# Function. Export a single-column Excel
# ------------------------------
export_single_column_list <- function(df, col_name, config, overwrite = TRUE) {
  validate_export_import(df, col_name)

  values <- get_unique_column(df, col_name)
  
  path <- generate_export_path(config, col_name, type = "lists")

  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "data")
  openxlsx::writeData(wb, "data", values)
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)

  path
}

# ------------------------------
# Function. Export multiple unique-column Excel files
# ------------------------------
export_selected_unique_lists <- function(df, config, overwrite = TRUE) {
  cols_to_export <- config$export_config$lists_to_export
  purrr::map(
    cols_to_export,
    ~ export_single_column_list(df, .x, config, overwrite)
  )
}

# ------------------------------
# End of script
# ------------------------------
