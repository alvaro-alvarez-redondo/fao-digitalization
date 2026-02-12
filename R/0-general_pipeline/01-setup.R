# ============================================================
# Script:  01-setup.R
# Purpose: Initialize project paths, configuration objects,
#          and required directory structure.
# ============================================================

# ------------------------------
# Global options
# ------------------------------
options(
  stringsAsFactors = FALSE, # Do not convert strings to factors
  scipen = 999 # Avoid scientific notation
)

# ------------------------------
# Function. Pipeline configuration
# ------------------------------
load_pipeline_config <- function() {
  project_root <- here::here()
  build_path <- function(...) fs::path(project_root, ...)

  # fixed project folders for this pipeline scope
  paths <- list(
    data = list(
      imports = list(
        raw = build_path("data", "imports", "raw imports"),
        cleaning = build_path("data", "imports", "cleaning imports"),
        harmonization = build_path("data", "imports", "harmonization imports")
      ),
      exports = list(
        lists = build_path("data", "exports", "lists"),
        processed = build_path("data", "exports", "processed data")
      )
    )
  )

  # fixed output names for a single-product pipeline
  files <- list(
    raw_data = "fao_data_raw.xlsx",
    wide_raw_data = "fao_data_wide_raw.xlsx",
    long_raw_data = "fao_data_long_raw.xlsx"
  )

  # semantic column groups for all transformations
  columns <- list(
    base = c("continent", "country", "unit", "footnotes"),
    id = c("product", "variable", "unit", "continent", "country", "footnotes"),
    value = c("year", "value"),
    system = c("notes", "yearbook", "document")
  )

  column_order <- columns |>
    unlist(use.names = FALSE) |>
    unique()

  # fixed export behavior for predictable output generation
  fixed_export_columns <- c(
    "product",
    "variable",
    "unit",
    "continent",
    "country",
    "footnotes",
    "year",
    "notes",
    "yearbook",
    "document"
  )

  export_config <- list(
    data_suffix = ".xlsx",
    list_suffix = "_unique.xlsx",
    lists_to_export = fixed_export_columns,
    lists_workbook_name = "fao_unique_lists_raw"
  )

  list(
    project_root = project_root,
    paths = paths,
    files = files,
    columns = columns,
    column_required = columns$base,
    column_id = columns$id,
    column_order = column_order,
    export_config = export_config,
    defaults = list(notes_value = NA_character_)
  )
}

# ------------------------------
# Function. Create required directories
# ------------------------------
create_required_directories <- function(paths) {
  all_directories <- paths |>
    unlist(recursive = TRUE, use.names = FALSE)

  fs::dir_create(all_directories)

  invisible(all_directories)
}

# ------------------------------
# End of script
# ------------------------------
