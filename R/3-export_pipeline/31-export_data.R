# script: 31-export_data.r
# description: export processed fao data to an excel workbook and return the output path.

#' @title export processed data
#' @description export a processed data frame to an excel workbook in the processed export path
#' generated from configuration values.
#'
#' @param fao_data_raw data frame containing processed fao records; validated with
#' `checkmate::assert_data_frame`.
#' @param config named list with export configuration values required by generate_export_path;
#' validated with `checkmate::assert_list`.
#' @param base_name single character string used as the export file base name; validated with
#' `checkmate::assert_string`.
#' @param overwrite logical flag indicating whether an existing file should be replaced;
#' validated with `checkmate::assert_flag`.
#' @return character scalar containing the generated file path for the exported excel workbook.
#' @importFrom checkmate assert_data_frame assert_list assert_string assert_flag
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' config <- list(output_dir = tempdir())
#' data_example <- data.frame(country = "argentina", value = 1)
#' export_processed_data(
#'   fao_data_raw = data_example,
#'   config = config,
#'   base_name = "data_export",
#'   overwrite = TRUE
#' )
export_processed_data <- function(
  fao_data_raw,
  config,
  base_name = "data_export",
  overwrite = TRUE
) {
  checkmate::assert_data_frame(fao_data_raw, min.rows = 1)
  checkmate::assert_list(config, names = "named")
  checkmate::assert_string(base_name, min.chars = 1)
  checkmate::assert_flag(overwrite)

  validate_export_import(fao_data_raw, base_name)

  path <- generate_export_path(config, base_name, type = "processed")

  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "data")
  openxlsx::writeData(wb, "data", fao_data_raw)
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)

  path
}
