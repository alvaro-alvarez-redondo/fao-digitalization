# script: setup script
# description: initializes global options and provides helpers to build the
# pipeline configuration object and create required project directories

options(
  stringsAsFactors = FALSE,
  scipen = 999
)

#' @title load pipeline config
#' @description builds and returns a deterministic configuration object for the
#' pipeline, including project-relative paths, file names, semantic column
#' groups, export settings, and default values used downstream.
#' @return named list with `project_root`, `paths`, `files`, `columns`,
#' `column_required`, `column_id`, `column_order`, `export_config`, and
#' `defaults`.
#' @importFrom here here
#' @importFrom fs path
#' @importFrom checkmate assert_string
#' @examples
#' config <- load_pipeline_config()
#' names(config)
load_pipeline_config <- function() {
  project_root <- here::here()
  checkmate::assert_string(project_root, min.chars = 1)

  build_path <- function(...) fs::path(project_root, ...)

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

  files <- list(
    raw_data = "fao_data_raw.xlsx",
    wide_raw_data = "fao_data_wide_raw.xlsx",
    long_raw_data = "fao_data_long_raw.xlsx"
  )

  columns <- list(
    base = c("continent", "country", "unit", "footnotes"),
    id = c("product", "variable", "unit", "continent", "country", "footnotes"),
    value = c("year", "value"),
    system = c("notes", "yearbook", "document")
  )

  column_order <- columns |>
    unlist(use.names = FALSE) |>
    unique()

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

#' @title create required directories
#' @description validates a nested list of directory paths, flattens it to a
#' character vector, creates every directory if missing, and returns the
#' resolved vector invisibly.
#' @param paths named or unnamed list containing character path elements. must
#' be a non-empty list that resolves to a non-empty character vector with no
#' missing values.
#' @return invisible character vector of directories passed to
#' `fs::dir_create()`.
#' @importFrom checkmate assert_list assert_character
#' @importFrom fs dir_create
#' @examples
#' temp_paths <- list(a = file.path(tempdir(), "a"), b = file.path(tempdir(), "b"))
#' create_required_directories(temp_paths)
create_required_directories <- function(paths) {
  checkmate::assert_list(paths, min.len = 1)

  all_directories <- paths |>
    unlist(recursive = TRUE, use.names = FALSE)

  checkmate::assert_character(all_directories, any.missing = FALSE, min.len = 1)

  fs::dir_create(all_directories)

  invisible(all_directories)
}
