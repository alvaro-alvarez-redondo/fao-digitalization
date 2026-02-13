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
# Function. Normalize worksheet names
# ------------------------------
normalize_sheet_name <- function(col_name) {
  assert_or_abort(checkmate::check_atomic(col_name, min.len = 1, any.missing = TRUE))

  sheet_name <- col_name |>
    normalize_filename() |>
    stringr::str_sub(1, 31)

  sheet_name[is.na(sheet_name) | sheet_name == ""] <- "sheet"

  sheet_name
}

# ------------------------------
# Function. Export all unique-column lists to one workbook
# ------------------------------
export_selected_unique_lists <- function(df, config, overwrite = TRUE) {
  validate_export_import(df, "fao_unique_lists_raw")

  cols_to_export <- config$export_config$lists_to_export
  workbook_path <- generate_export_path(
    config,
    config$export_config$lists_workbook_name,
    type = "lists"
  )

  wb <- openxlsx::createWorkbook()

  purrr::walk(cols_to_export, function(col_name) {
    values <- get_unique_column(df, col_name)
    sheet_name <- normalize_sheet_name(col_name)
    openxlsx::addWorksheet(wb, sheet_name)
    openxlsx::writeData(wb, sheet_name, values)
  })

  openxlsx::saveWorkbook(wb, workbook_path, overwrite = overwrite)

  workbook_path
}

# ------------------------------
# End of script
# ------------------------------
