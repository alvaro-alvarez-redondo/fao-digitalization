# script: setup script
# description: initializes global options and provides helpers to build the
# pipeline configuration object and create required project directories

options(
  stringsAsFactors = FALSE,
  scipen = 999
)

#' @title load pipeline config
#' @description builds and returns a deterministic configuration object for the
#' pipeline, including project-root-relative paths, file names, semantic column
#' groups, export settings, default values, and dataset-specific audit paths.
#' @param dataset_name character scalar dataset identifier used to build
#' audit directories and audit workbook names with the
#' `{dataset_name}_audit.xlsx` convention.
#' @param ... reserved for future optional configuration overrides.
#' @return named list with `project_root`, `dataset_name`, `paths`, `files`,
#' `columns`, `column_required`, `column_id`, `column_order`, `export_config`,
#' `defaults`, and `messages`. `paths$data$audit` contains
#' `dataset_dir`, `audit_file_path`, and `raw_imports_mirror_dir`.
#' @importFrom here here
#' @importFrom fs dir_create path
#' @importFrom checkmate assert_string assert_directory_exists
#' @importFrom cli cli_abort
#' @importFrom purrr walk
#' @examples
#' config <- load_pipeline_config("fao_data_raw")
#' names(config)
load_pipeline_config <- function(dataset_name = "fao_data_raw", ...) {
  checkmate::assert_string(dataset_name, min.chars = 1)

  normalized_dataset_name <- dataset_name |>
    as.character() |>
    tolower() |>
    iconv(from = "", to = "ascii//translit")

  normalized_dataset_name <- gsub("[^a-z0-9 ]", " ", normalized_dataset_name)
  normalized_dataset_name <- gsub("\\s+", "_", trimws(normalized_dataset_name))

  if (is.na(normalized_dataset_name) || normalized_dataset_name == "") {
    cli::cli_abort("{.arg dataset_name} must resolve to a non-empty normalized value")
  }

  project_root <- here::here()
  checkmate::assert_string(project_root, min.chars = 1)

  build_path <- function(...) fs::path(project_root, ...)

  required_base_directories <- c(
    build_path("data"),
    build_path("data", "imports")
  )

  fs::dir_create(required_base_directories)
  purrr::walk(required_base_directories, checkmate::assert_directory_exists)

  raw_imports_dir <- build_path("data", "imports", "raw imports")
  audit_dataset_dir <- build_path("data", "audit", normalized_dataset_name)

  paths <- list(
    data = list(
      imports = list(
        raw = raw_imports_dir,
        cleaning = build_path("data", "imports", "cleaning imports"),
        harmonization = build_path("data", "imports", "harmonization imports")
      ),
      exports = list(
        lists = build_path("data", "exports", "lists"),
        processed = build_path("data", "exports", "processed data")
      ),
      audit = list(
        dataset_dir = audit_dataset_dir,
        audit_file_path = fs::path(
          audit_dataset_dir,
          paste0(normalized_dataset_name, "_audit.xlsx")
        ),
        raw_imports_mirror_dir = fs::path(audit_dataset_dir, "raw_imports_mirror")
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

  column_order <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "value",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

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
    dataset_name = normalized_dataset_name,
    paths = paths,
    files = files,
    columns = columns,
    column_required = columns$base,
    column_id = columns$id,
    column_order = column_order,
    export_config = export_config,
    defaults = list(notes_value = NA_character_),
    messages = list(show_missing_product_metadata_warning = FALSE)
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
