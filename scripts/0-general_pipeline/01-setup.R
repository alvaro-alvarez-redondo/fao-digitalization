# script: setup script
# description: initializes global options and provides helpers to build the
# pipeline configuration object and create required project directories

options(
  stringsAsFactors = FALSE,
  scipen = 999
)


#' @title Get centralized pipeline constants
#' @description Returns deterministic hard-coded constants used across pipeline
#' scripts, including option keys, script names, object names, and message
#' literals.
#' @return Named list of centralized constants.
#' @examples
#' constants <- get_pipeline_constants()
#' names(constants)
.pipeline_constants_cache <- NULL

get_pipeline_constants <- function() {
  # Cache is safe for parallel contexts: future workers start fresh R sessions
  # with independent global environments, so no cross-worker race conditions.
  if (!is.null(.pipeline_constants_cache)) {
    return(.pipeline_constants_cache)
  }

  constants <- list(
    dataset_default_name = "whep_data_raw",
    timestamp_format_utc = "%Y-%m-%dT%H:%M:%SZ",
    na_placeholder = "..NA_INTERNAL..",
    na_match_key = "..NA_MATCH_KEY..",
    auto_run_options = list(
      pipeline = "whep.run_pipeline.auto",
      general = "whep.run_general_pipeline.auto",
      import = "whep.run_import_pipeline.auto",
      post_processing = "whep.run_post_processing_pipeline.auto",
      export = "whep.run_export_pipeline.auto"
    ),
    toggle_options = list(
      drop_na_values = "whep.drop_na_values"
    ),
    patterns = list(
      normalize_non_alnum = "[^a-z0-9]+",
      normalize_already_clean = "^([a-z0-9]+( [a-z0-9]+)*)?$",
      year_column = "^\\d{4}(-\\d{4})?$"
    ),
    performance = list(
      normalize_unique_min_n = 256L,
      normalize_unique_sample_n = 2048L,
      normalize_unique_ratio_threshold = 0.85
    ),
    defaults = list(
      unknown_document = "unknown_document"
    ),
    script_names = list(
      general = c("00-dependencies.R", "01-setup.R", "02-helpers.R"),
      pipeline_stage_runners = c(
        "run_general_pipeline.R",
        "run_import_pipeline.R",
        "run_post_processing_pipeline.R",
        "run_export_pipeline.R"
      )
    ),
    object_names = list(
      raw = "whep_data_raw",
      wide_raw = "whep_data_wide_raw",
      cleaned = "whep_data_cleaned",
      normalized = "whep_data_normalized",
      harmonized = "whep_data_harmonized",
      export_paths = "export_paths",
      collected_reading_errors = "collected_reading_errors",
      collected_errors = "collected_errors",
      collected_warnings = "collected_warnings"
    ),
    helper_requirements = list(
      assignment_helper = "assign_environment_values",
      assignment_helper_source = "scripts/0-general_pipeline/02-helpers.R"
    )
  )

  .pipeline_constants_cache <<- constants

  return(constants)
}

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
#' `whep_data_raw`.
#' @param ... optional values. if a named argument `data` is provided and has a
#' `dataset_name` attribute, it is used as a fallback source for dataset naming.
#' @return named list with `project_root`, `dataset_name`, `paths`, `files`,
#' `columns`, `column_required`, `column_id`, `column_order`, `export_config`,
#' `defaults`, and `messages`. `export_config$styles$error_highlight` defines
#' centralized workbook styling for invalid audit cells. `paths$data$audit` contains
#' `audit_root_dir`,
#' `audit_dir`, `audit_file_name`, `audit_file_path`, and
#' `raw_imports_mirror_dir` for easy direct access or recursive unlisting.
#' @importFrom here here
#' @importFrom fs dir_create path
#' @importFrom checkmate assert_string assert_directory_exists
#' @importFrom cli cli_abort
#' @importFrom purrr walk
#' @examples
#' config <- load_pipeline_config("whep_data_raw")
#' names(config)
load_pipeline_config <- function(
  dataset_name = get_pipeline_constants()$dataset_default_name,
  ...
) {
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
    resolved_dataset_name <- get_pipeline_constants()$dataset_default_name
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

  build_path <- function(...) {
    return(fs::path(project_root, ...))
  }

  raw_imports_dir <- build_path("data", "1-import", "10-raw_imports")
  audit_root_dir <- build_path("data", "2-post_processing")
  audit_dir <- fs::path(audit_root_dir, "data_audit")
  audit_file_name <- paste0(normalized_dataset_name, "_audit.xlsx")

  paths <- list(
    data = list(
      imports = list(
        raw = raw_imports_dir,
        cleaning = build_path("data", "1-import", "11-clean_imports"),
        standardization = build_path(
          "data",
          "1-import",
          "12-standardize_imports"
        ),
        harmonization = build_path("data", "1-import", "13-harmonize_imports")
      ),
      exports = list(
        lists = build_path("data", "3-export", "lists"),
        processed = build_path("data", "3-export", "processed_data")
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
    raw_data = "whep_data_raw.xlsx",
    wide_raw_data = "whep_data_wide_raw.xlsx",
    long_raw_data = "whep_data_long_raw.xlsx"
  )

  columns <- list(
    base = c("continent", "country", "unit", "footnotes"),
    id = c(
      "product", "variable", "unit", "hemisphere",
      "continent", "country", "footnotes"
    ),
    value = c("year", "value"),
    system = c("notes", "yearbook", "document")
  )

  column_order <- c(
    "hemisphere",
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
    "hemisphere",
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
    lists_workbook_name = "whep_unique_lists_raw",
    export_layers = c("harmonized"),
    styles = list(
      error_highlight = list(
        fgFill = "#FFB84D",
        fontColour = "#000000",
        textDecoration = "bold",
        border = "TopBottomLeftRight",
        borderColour = "#6D4C41",
        borderStyle = "thick"
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

  return(config)
}

#' @title resolve audit root directory
#' @description safely extracts the optional audit root directory from a
#' pipeline-like `paths` list without assuming nested members are present.
#' @param paths named or unnamed list that may contain
#' `paths$data$audit$audit_root_dir`.
#' @return character scalar audit root directory when configured; otherwise
#' `NULL`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom purrr pluck
#' @examples
#' resolve_audit_root_dir(list())
resolve_audit_root_dir <- function(paths) {
  checkmate::assert_list(paths)

  audit_root_dir <- purrr::pluck(
    paths,
    "data",
    "audit",
    "audit_root_dir",
    .default = NULL
  )

  if (!is.null(audit_root_dir)) {
    checkmate::assert_string(audit_root_dir, min.chars = 1)
  }

  return(audit_root_dir)
}


#' @title ensure directories exist
#' @description creates directories in deterministic sorted order.
#' @param directories character vector of directory paths.
#' @param recurse logical scalar passed to `fs::dir_create()`.
#' @return invisible character vector of created directory paths.
#' @importFrom checkmate assert_character assert_flag
#' @importFrom fs dir_create
#' @examples
#' ensure_directories_exist(file.path(tempdir(), c("a", "b")))
ensure_directories_exist <- function(directories, recurse = TRUE) {
  checkmate::assert_character(directories, any.missing = FALSE)
  checkmate::assert_flag(recurse)

  if (length(directories) == 0) {
    return(invisible(character(0)))
  }

  normalized_directories <- directories |>
    unique() |>
    sort()

  fs::dir_create(normalized_directories, recurse = recurse)

  return(invisible(normalized_directories))
}

#' @title delete directory if it exists
#' @description deletes a directory path when it exists and optionally tolerates
#' permission errors while keeping deterministic error handling.
#' @param directory character scalar directory path.
#' @param tolerate_permission_errors logical scalar; when `TRUE`, permission
#' and lock-related deletion errors return `FALSE` instead of aborting.
#' @return invisible logical scalar indicating whether a directory was deleted.
#' @importFrom checkmate assert_string assert_flag
#' @importFrom cli cli_abort
#' @importFrom fs dir_exists dir_delete
#' @importFrom purrr safely
#' @examples
#' delete_directory_if_exists(file.path(tempdir(), "nonexistent"))
delete_directory_if_exists <- function(
  directory,
  tolerate_permission_errors = FALSE
) {
  checkmate::assert_string(directory, min.chars = 1)
  checkmate::assert_flag(tolerate_permission_errors)

  if (!fs::dir_exists(directory)) {
    return(invisible(FALSE))
  }

  delete_result <- purrr::safely(fs::dir_delete)(directory)

  if (!is.null(delete_result$error)) {
    error_message <- as.character(delete_result$error$message)
    permission_error <- grepl(
      "EPERM|permission denied|operation not permitted|access is denied",
      error_message,
      ignore.case = TRUE
    )

    if (tolerate_permission_errors && permission_error) {
      return(invisible(FALSE))
    }

    cli::cli_abort(
      "failed to delete existing folder {.path {directory}}: {error_message}"
    )
  }

  return(invisible(TRUE))
}

#' @title create required directories
#' @description validates a nested list of paths, flattens it to a character
#' vector, normalizes file paths to their parent directories, excludes audit
#' directories for lazy creation, creates every remaining directory if missing,
#' and returns the resolved directory vector invisibly.
#' @param paths named or unnamed list containing character path elements. must
#' be a non-empty list that resolves to a non-empty character vector with no
#' missing values.
#' @return invisible character vector of directories passed to
#' `fs::dir_create()`.
#' @importFrom checkmate assert_list assert_character
#' @importFrom fs dir_create path_file path_dir path_norm
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
    vapply(
      \(path_value) {
        path_file_name <- fs::path_file(path_value)

        if (grepl("\\.[a-z0-9]+$", path_file_name)) {
          return(fs::path_dir(path_value))
        }

        path_value
      },
      character(1)
    ) |>
    unique() |>
    sort()

  audit_root_dir <- resolve_audit_root_dir(paths)

  if (is.character(audit_root_dir) && length(audit_root_dir) == 1) {
    normalized_audit_root <- fs::path_norm(audit_root_dir)
    all_directories <- all_directories[
      !vapply(
        all_directories,
        \(path_value) {
          normalized_path <- fs::path_norm(path_value)
          identical(normalized_path, normalized_audit_root) ||
            startsWith(
              normalized_path,
              paste0(normalized_audit_root, .Platform$file.sep)
            )
        },
        logical(1)
      )
    ]
  }

  if (length(all_directories) > 0) {
    ensure_directories_exist(all_directories, recurse = TRUE)
  }

  return(invisible(all_directories))
}

#' @title ensure output directories
#' @description creates parent directories for generated output files only when
#' at least one file path is provided.
#' @param output_paths character vector of file paths that will be generated.
#' @return invisible character vector of parent directories that were created.
#' @importFrom checkmate assert_character
#' @importFrom fs dir_create path_dir
#' @examples
#' output_paths <- file.path(tempdir(), "audit", "dataset", "result.xlsx")
#' ensure_output_directories(output_paths)
ensure_output_directories <- function(output_paths) {
  checkmate::assert_character(output_paths, any.missing = FALSE)

  if (length(output_paths) == 0) {
    return(invisible(character(0)))
  }

  output_directories <- unique(fs::path_dir(output_paths))
  ensure_directories_exist(output_directories, recurse = TRUE)

  return(invisible(output_directories))
}
