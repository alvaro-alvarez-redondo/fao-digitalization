# ============================================================
# Script:  31-export_data.R
# Purpose: Export the final consolidated data.table to a single
#          Excel file in the processed data export folder.
# ============================================================

# ------------------------------
# Function. Export final data.table to Excel
# ------------------------------
export_processed_data <- function(
  fao_data_raw,
  config,
  base_name = "data_export",
  overwrite = TRUE
) {
  # Validate imports
  validate_export_import(fao_data_raw, base_name)

  # Generate export path
  path <- generate_export_path(config, base_name, type = "processed")

  # Create workbook and write data
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "data")
  openxlsx::writeData(wb, "data", fao_data_raw)

  # Save workbook
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)

  path
}

# ------------------------------
# End of script
# ------------------------------
