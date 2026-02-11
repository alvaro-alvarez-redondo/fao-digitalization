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
load_pipeline_config <- function(base_path = here::here()) {
  project_path <- function(...) fs::path(base_path, ...)

  # ------------------------------
  # 1. Project structure
  # ------------------------------
  paths <- list(
    data = list(
      imports = list(
        raw = project_path("data", "imports", "raw imports"),
        cleaning = project_path("data", "imports", "cleaning imports"),
        harmonization = project_path("data", "imports", "harmonization imports")
      ),
      exports = list(
        lists = project_path("data", "exports", "lists"),
        processed = project_path("data", "exports", "processed data")
      )
    )
  )

  # ------------------------------
  # 2. Key filenames
  # ------------------------------
  files <- list(
    fao_final = "fao_data_final.xlsx",
    fao_wide = "fao_data_wide.xlsx",
    fao_long = "fao_data_long.xlsx"
  )

  # ------------------------------
  # 3. Column configuration (bloques semánticos)
  # ------------------------------
  columns <- list(
    # Metadata coming from source
    base = c("continent", "country", "unit", "footnotes"),

    # Identifiers for reshaping
    id = c("product", "variable", "unit", "continent", "country", "footnotes"),

    # Value columns
    value = c("year", "value"),

    # System-generated metadata
    system = c("notes", "yearbook", "document")
  )

  column_order <- c(columns$id, columns$value, columns$system)

export_config <- list(
  data_suffix = ".xlsx",
  list_suffix = "_unique.xlsx",
  lists_to_export = c(columns$id, "year", columns$system)
)

  # ------------------------------
  # 5. Defaults
  # ------------------------------
  defaults <- list(
    notes_value = NA_character_
  )

  # ------------------------------
  # 6. Return full configuration
  # ------------------------------
  list(
    project_root = base_path,
    paths = paths,
    files = files,
    export_config = export_config,
    defaults = defaults,
    columns = columns,
    column_order = column_order,
    column_required = columns$base,
    column_id = columns$id
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
