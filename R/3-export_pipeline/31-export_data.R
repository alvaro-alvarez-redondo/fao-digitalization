# ============================================================
# Script:  31-export_data.R
# Purpose: Export the final consolidated data.table to a single
#          Excel file in the processed data export folder.
# ============================================================

# ------------------------------
# Function. Export final data.table to Excel
# ------------------------------
export_processed_data <- function(
  final_dt,
  config,
  base_name = "final",
  overwrite = TRUE
) {
  # Validate imports
  validate_export_import(final_dt, base_name)

  # Generate export path
  path <- generate_excel_path(config, base_name)

  # Create workbook and write data
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "data")
  openxlsx::writeData(wb, "data", final_dt)

  # Save workbook
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)

  path
}

# ------------------------------
# End of script
# ------------------------------
