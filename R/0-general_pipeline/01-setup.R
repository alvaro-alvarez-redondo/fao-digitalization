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

  project_path <- function(...) fs::path(project_root, ...)

  paths <- list(
    data = list(
      imports = list(
        raw = fs::path(project_root, "data", "imports", "raw imports"),
        cleaning = fs::path(project_root, "data", "imports", "cleaning imports"),
        harmonization = fs::path(
          project_root,
          "data",
          "imports",
          "harmonization imports"
        )
      ),
      exports = list(
        lists = fs::path(project_root, "data", "exports", "lists"),
        processed = fs::path(project_root, "data", "exports", "processed data")
      )
    )
  )

  files <- list(
    fao_final = "fao_data_final.xlsx",
    fao_wide = "fao_data_wide.xlsx",
    fao_long = "fao_data_long.xlsx"
  )

  # ------------------------------
  # Exports configuration for subpipeline
  # ------------------------------
  export_config <- list(
    excel_suffix = "_export.xlsx", # Suffix for full data export
    list_suffix = "_unique.xlsx", # Suffix for unique-column lists

    lists_to_export = c(
      "continent",
      "country",
      "unit",
      "product",
      "variable"
      # Add or remove columns as needed
    )
  )

  fs::dir_create(unlist(paths, recursive = TRUE, use.names = FALSE))

  list(
    project_root = project_root,
    paths = paths,
    files = files,
    export_config = export_config,
    defaults = list(
      notes_value = NA_character_
    ),
    column_order = c(
      "product",
      "variable",
      "unit",
      "continent",
      "country",
      "year",
      "value",
      "footnotes",
      "notes",
      "yearbook",
      "document"
    )
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
