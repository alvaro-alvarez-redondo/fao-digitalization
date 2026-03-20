# test_helper.R
# shared test utilities and fixtures for the FAO pipeline test suite
#
# this file must be sourced before running any test file. it:
# 1. disables all auto-run options to prevent side effects
# 2. sources the general pipeline scripts (setup, helpers)
# 3. provides shared fixture builders used across test files

options(
  fao.run_pipeline.auto            = FALSE,
  fao.run_general_pipeline.auto    = FALSE,
  fao.run_import_pipeline.auto     = FALSE,
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_export_pipeline.auto     = FALSE,
  fao.checkpointing.enabled        = FALSE
)

source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)
source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)


# --- shared fixture builders ------------------------------------------------

#' Build a temporary test directory and return its path.
#' The directory is created and will be cleaned up when the test finishes.
build_temp_dir <- function(pattern = "fao-test-") {
  dir_path <- tempfile(pattern)
  dir.create(dir_path, recursive = TRUE)
  return(dir_path)
}

#' Build a minimal pipeline config pointing at temporary directories.
build_test_config <- function(root_dir = NULL) {
  if (is.null(root_dir)) {
    root_dir <- build_temp_dir("fao-config-")
  }

  raw_dir           <- file.path(root_dir, "data", "1-import", "10-raw_imports")
  cleaning_dir      <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  standardization_dir <- file.path(root_dir, "data", "1-import", "12-standardize_imports")
  harmonization_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  processed_dir     <- file.path(root_dir, "data", "3-export", "processed_data")
  lists_dir         <- file.path(root_dir, "data", "3-export", "lists")
  audit_root_dir    <- file.path(root_dir, "data", "2-post_processing")

  dirs <- c(
    raw_dir, cleaning_dir, standardization_dir, harmonization_dir,
    processed_dir, lists_dir, audit_root_dir
  )
  for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

  list(
    project_root = root_dir,
    paths = list(
      data = list(
        imports = list(
          raw              = raw_dir,
          cleaning         = cleaning_dir,
          standardization  = standardization_dir,
          harmonization    = harmonization_dir
        ),
        exports = list(
          processed = processed_dir,
          lists     = lists_dir
        ),
        audit = list(
          audit_root_dir       = audit_root_dir,
          audit_dir            = file.path(audit_root_dir, "data_audit"),
          audit_file_name      = "fao_data_raw_audit.xlsx",
          audit_file_path      = file.path(audit_root_dir, "data_audit", "fao_data_raw_audit.xlsx"),
          raw_imports_mirror_dir = file.path(audit_root_dir, "data_audit", "raw_imports_mirror")
        )
      )
    ),
    column_required = c("continent", "country"),
    column_id       = c("product", "variable", "unit", "continent", "country", "footnotes"),
    column_order    = c(
      "hemisphere", "continent", "country", "product", "variable",
      "unit", "year", "value", "notes", "footnotes",
      "yearbook", "document"
    ),
    defaults = list(notes_value = NA_character_),
    messages = list(show_missing_product_metadata_warning = FALSE),
    export_config = list(
      data_suffix = ".xlsx",
      list_suffix = "_list.xlsx",
      layer_suffixes = c("_raw", "_cleaned", "_normalized", "_harmonized"),
      export_layers = c("harmonized"),
      styles = list(
        error_highlight = list(fontColour = "#9C0006", bgFill = "#FFC7CE")
      )
    )
  )
}

#' Create a minimal test Excel file with the given data.
#' Returns the path to the created file.
create_test_xlsx <- function(data, file_path, sheet_name = "Sheet1") {
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, sheet_name)
  openxlsx::writeData(wb, sheet_name, data)
  openxlsx::saveWorkbook(wb, file_path, overwrite = TRUE)
  return(file_path)
}

#' Build a sample long-format data.table for testing transformations.
build_sample_long_dt <- function(n_rows = 4L) {
  data.table::data.table(
    continent = rep(c("Asia", "Europe"), length.out = n_rows),
    country   = rep(c("Japan", "France"), length.out = n_rows),
    product   = rep("wheat", n_rows),
    variable  = rep("production", n_rows),
    unit      = rep("tonnes", n_rows),
    year      = as.character(2020L + seq_len(n_rows) - 1L),
    value     = as.character(seq_len(n_rows) * 100L),
    notes     = rep(NA_character_, n_rows),
    footnotes = rep(NA_character_, n_rows),
    yearbook  = rep("yb_2024", n_rows),
    document  = rep("test_file.xlsx", n_rows)
  )
}
