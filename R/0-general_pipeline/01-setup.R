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
#' audit paths are generated dynamically from `dataset_name` so the auditing
#' workflow is reusable across multiple datasets.
#' @param dataset_name character scalar dataset identifier used to build
#' audit directories and audit workbook names with the
#' `{dataset_name}_audit.xlsx` convention. when `null` or empty, the function
#' attempts to derive a name from `data` attributes in `...` and falls back to
#' `fao_data_raw`.
#' @param ... optional values. if a named argument `data` is provided and has a
#' `dataset_name` attribute, it is used as a fallback source for dataset naming.
#' @return named list with `project_root`, `dataset_name`, `paths`, `files`,
#' `columns`, `column_required`, `column_id`, `column_order`, `export_config`,
#' `defaults`, and `messages`. `export_config$styles$error_highlight` defines
#' centralized workbook styling for invalid audit cells. `paths$data$audit` contains `audit_root_dir`,
#' `audit_dir`, `audit_file_name`, `audit_file_path`, and
#' `raw_imports_mirror_dir` for easy direct access or recursive unlisting.
#' @importFrom here here
#' @importFrom fs dir_create path
#' @importFrom checkmate assert_string assert_directory_exists
#' @importFrom cli cli_abort cli_inform
#' @importFrom purrr walk
#' @examples
#' config <- load_pipeline_config("fao_data_raw")
#' names(config)
load_pipeline_config <- function(dataset_name = "fao_data_raw", ...) {
  optional_args <- list(...)

  inferred_dataset_name <- NULL

  if (!is.null(optional_args$data)) {
    inferred_dataset_name <- attr(
      optional_args$data,
      "dataset_name",
      exact = TRUE
    )

    if (is.null(inferred_dataset_name) && !is.null(names(optional_args$data))) {
      inferred_dataset_name <- attr(optional_args$data, "name", exact = TRUE)
    }
  }

  resolved_dataset_name <- dataset_name

  if (
    is.null(resolved_dataset_name) ||
      !nzchar(trimws(as.character(resolved_dataset_name)))
  ) {
    resolved_dataset_name <- inferred_dataset_name
  }

  if (
    is.null(resolved_dataset_name) ||
      !nzchar(trimws(as.character(resolved_dataset_name)))
  ) {
    resolved_dataset_name <- "fao_data_raw"
  }

  checkmate::assert_string(resolved_dataset_name, min.chars = 1)

  normalized_dataset_name <- resolved_dataset_name |>
    as.character() |>
    tolower() |>
    iconv(from = "", to = "ascii//translit")

  normalized_dataset_name <- gsub("[^a-z0-9 ]", " ", normalized_dataset_name)
  normalized_dataset_name <- gsub("\\s+", "_", trimws(normalized_dataset_name))

  if (is.na(normalized_dataset_name) || normalized_dataset_name == "") {
    cli::cli_abort(
      "{.arg dataset_name} must resolve to a non-empty normalized value"
    )
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
  audit_root_dir <- build_path("data", "audit")
  audit_dir <- fs::path(audit_root_dir, normalized_dataset_name)
  audit_file_name <- paste0(normalized_dataset_name, "_audit.xlsx")

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
        audit_root_dir = audit_root_dir,
        audit_dir = audit_dir,
        dataset_dir = audit_dir,
        audit_file_name = audit_file_name,
        audit_file_path = fs::path(audit_dir, audit_file_name),
        raw_imports_mirror_dir = fs::path(audit_dir, "raw_imports_mirror")
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
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

  audit_columns <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "yearbook",
    "document"
  )

  export_config <- list(
    data_suffix = ".xlsx",
    list_suffix = "_unique.xlsx",
    lists_to_export = fixed_export_columns,
    lists_workbook_name = "fao_unique_lists_raw",
    styles = list(
      error_highlight = list(
        fgFill = "#ffff00",
        fontColour = "#000000",
        textDecoration = "bold"
      )
    )
  )

  config <- list(
    project_root = project_root,
    dataset_name = normalized_dataset_name,
    paths = paths,
    files = files,
    columns = columns,
    column_required = columns$base,
    column_id = columns$id,
    column_order = column_order,
    export_config = export_config,
    audit_columns = audit_columns,
    defaults = list(notes_value = NA_character_),
    messages = list(show_missing_product_metadata_warning = FALSE)
  )

  checkmate::assert_character(
    config$column_order,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_character(
    config$audit_columns,
    min.len = 1,
    any.missing = FALSE
  )

  if (!is.null(config$audit_columns_by_type)) {
    checkmate::assert_list(
      config$audit_columns_by_type,
      min.len = 1,
      any.missing = FALSE
    )
    purrr::walk(config$audit_columns_by_type, \(audit_columns) {
      checkmate::assert_character(
        audit_columns,
        min.len = 1,
        any.missing = FALSE
      )
    })
  }

  checkmate::assert_string(
    config$paths$data$imports$raw,
    min.chars = 1
  )
  checkmate::assert_string(
    config$paths$data$audit$audit_file_path,
    min.chars = 1
  )
  checkmate::assert_string(
    config$paths$data$audit$raw_imports_mirror_dir,
    min.chars = 1
  )
  checkmate::assert_list(
    config$export_config$styles,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_list(
    config$export_config$styles$error_highlight,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_string(
    config$export_config$styles$error_highlight$fgFill,
    min.chars = 1
  )
  checkmate::assert_string(
    config$export_config$styles$error_highlight$fontColour,
    min.chars = 1
  )
  checkmate::assert_string(
    config$export_config$styles$error_highlight$textDecoration,
    min.chars = 1
  )

  config
}

#' @title create required directories
#' @description validates a nested list of paths, flattens it to a
#' character vector, normalizes file paths to their parent directories, creates
#' every directory if missing, and returns the resolved directory vector
#' invisibly.
#' @param paths named or unnamed list containing character path elements. must
#' be a non-empty list that resolves to a non-empty character vector with no
#' missing values.
#' @return invisible character vector of directories passed to
#' `fs::dir_create()`.
#' @importFrom checkmate assert_list assert_character
#' @importFrom fs dir_create path_file path_dir
#' @importFrom purrr map_chr
#' @examples
#' temp_paths <- list(a = file.path(tempdir(), "a"), b = file.path(tempdir(), "b"))
#' create_required_directories(temp_paths)
create_required_directories <- function(paths) {
  checkmate::assert_list(paths, min.len = 1)

  all_paths <- paths |>
    unlist(recursive = TRUE, use.names = FALSE)

  checkmate::assert_character(all_paths, any.missing = FALSE, min.len = 1)

  all_directories <- all_paths |>
    purrr::map_chr(\(path_value) {
      path_file_name <- fs::path_file(path_value)

      if (grepl("\\.[a-z0-9]+$", path_file_name)) {
        return(fs::path_dir(path_value))
      }

      path_value
    }) |>
    unique()

  fs::dir_create(all_directories)

  invisible(all_directories)
}
