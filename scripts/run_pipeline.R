if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)
}

#' @title Run full project pipeline
#' @description Runs the general, import, post-processing, and export pipeline
#'   scripts in deterministic sequence.
#'
#' @param show_view Logical scalar. If `TRUE`, display `fao_data_raw` in the
#'   RStudio viewer when the object exists.
#' @param pipeline_root Character scalar. Root folder containing pipeline
#'   scripts.
#'
#' @return Invisibly returns `TRUE` when all pipeline scripts execute
#'   successfully.
#'
#' @examples
#' \dontrun{
#' run_pipeline(show_view = FALSE)
#' }
#' @export
run_pipeline <- function(
  show_view = interactive(),
  pipeline_root = here::here("scripts")
) {
  assert_pipeline_runtime_dependencies()

  checkmate::assert_flag(show_view)
  checkmate::assert_string(pipeline_root, na.ok = FALSE, min.chars = 1)

  normalized_pipeline_root <- normalizePath(
    path = pipeline_root,
    winslash = "/",
    mustWork = FALSE
  )

  if (!dir.exists(normalized_pipeline_root)) {
    cli::cli_abort(
      "pipeline root does not exist: {.path {normalized_pipeline_root}}"
    )
  }

  pipeline_files <- resolve_pipeline_files(
    pipeline_root = normalized_pipeline_root
  )

  purrr::walk(pipeline_files, run_pipeline_script)

  maybe_view_pipeline_output(show_view = show_view)

  return(invisible(TRUE))
}

#' @title Assert runtime package dependencies for pipeline orchestration
#' @description Validates availability of required namespaces used by
#'   `run_pipeline()` without attaching packages to the search path.
#' @return Invisibly returns `TRUE` when all namespaces are available.
#' @keywords internal
assert_pipeline_runtime_dependencies <- function() {
  required_namespaces <- c("checkmate", "cli", "here", "purrr")

  missing_namespaces <- required_namespaces[
    !vapply(required_namespaces, requireNamespace, logical(1), quietly = TRUE)
  ]

  if (length(missing_namespaces) > 0L) {
    cli::cli_abort(
      c(
        "missing required pipeline dependencies.",
        "x" = "install missing package(s): {.val {missing_namespaces}}"
      )
    )
  }

  return(invisible(TRUE))
}

#' @title Get expected ordered stage runner names
#' @description Returns the canonical ordered stage runner file names used by
#'   the pipeline orchestration contract.
#' @return Character vector of expected stage runner file names.
#' @keywords internal
get_expected_stage_runner_names <- function() {
  return(c(
    "run_general_pipeline.R",
    "run_import_pipeline.R",
    "run_post_processing_pipeline.R",
    "run_export_pipeline.R"
  ))
}

#' @title Validate configured stage runner names
#' @description Ensures configured stage runner names match the canonical
#'   deterministic contract.
#' @param stage_runner_names Character vector from configuration constants.
#' @return Invisibly returns `TRUE` when validation succeeds.
#' @keywords internal
validate_stage_runner_names <- function(stage_runner_names) {
  checkmate::assert_character(
    stage_runner_names,
    len = 4,
    any.missing = FALSE,
    unique = TRUE
  )

  expected_stage_runner_names <- get_expected_stage_runner_names()

  if (!identical(stage_runner_names, expected_stage_runner_names)) {
    cli::cli_abort(
      c(
        "invalid pipeline stage runner configuration.",
        "x" = "expected ordered stage runner names: {.val {expected_stage_runner_names}}",
        "i" = "configured names: {.val {stage_runner_names}}"
      )
    )
  }

  return(invisible(TRUE))
}

#' @title Resolve stage directories
#' @description Returns canonical stage directory names aligned by index with
#'   stage runner names.
#' @return Character vector with deterministic pipeline stage folder names.
#' @keywords internal
get_pipeline_stage_directories <- function() {
  return(c(
    "0-general_pipeline",
    "1-import_pipeline",
    "2-post_processing_pipeline",
    "3-export_pipeline"
  ))
}

#' @title Resolve pipeline script paths
#' @description Builds ordered script paths for all pipeline stages.
#' @param pipeline_root Character scalar existing directory.
#' @return Character vector of script paths in execution order.
#' @keywords internal
resolve_pipeline_files <- function(pipeline_root) {
  checkmate::assert_string(pipeline_root, na.ok = FALSE, min.chars = 1)
  checkmate::assert_directory_exists(pipeline_root)

  pipeline_constants <- get_pipeline_constants()
  stage_runner_names <- pipeline_constants$script_names$pipeline_stage_runners
  validate_stage_runner_names(stage_runner_names)

  pipeline_files <- file.path(
    pipeline_root,
    get_pipeline_stage_directories(),
    stage_runner_names
  )

  return(pipeline_files)
}

#' @title Source an individual pipeline script
#' @description Validates path existence, logs script execution, and sources the
#'   script.
#' @param pipeline_file Character scalar path to pipeline script.
#' @return Invisibly returns `TRUE`.
#' @keywords internal
run_pipeline_script <- function(pipeline_file) {
  checkmate::assert_string(pipeline_file, na.ok = FALSE, min.chars = 1)

  if (!file.exists(pipeline_file)) {
    cli::cli_abort(
      "{.strong required pipeline script is missing: {.path {pipeline_file}}}"
    )
  }

  pipeline_name <- basename(pipeline_file)
  cli::cli_alert_info(
    "{.strong running pipeline script: {.val {pipeline_name}}}"
  )

  source(pipeline_file, local = FALSE, echo = FALSE)

  return(invisible(TRUE))
}

#' @title Optionally view pipeline output object
#' @description Opens the most advanced available pipeline dataset in RStudio
#'   viewer if requested.
#' @param show_view Logical scalar controlling view behavior.
#' @return Invisibly returns `TRUE`.
#' @keywords internal
maybe_view_pipeline_output <- function(show_view) {
  checkmate::assert_flag(show_view)

  if (show_view) {
    pipeline_constants <- get_pipeline_constants()
    object_priority <- c(
      pipeline_constants$object_names$harmonized,
      pipeline_constants$object_names$normalized,
      pipeline_constants$object_names$cleaned,
      pipeline_constants$object_names$raw
    )

    available_objects <- object_priority[vapply(
      object_priority,
      exists,
      logical(1),
      inherits = TRUE
    )]

    available_object <- if (length(available_objects) > 0L) {
      available_objects[[1L]]
    } else {
      NA_character_
    }

    if (!is.na(available_object) && nzchar(available_object)) {
      utils::View(get(available_object, inherits = TRUE))
    }
  }

  return(invisible(TRUE))
}

if (isTRUE(getOption(get_pipeline_constants()$auto_run_options$pipeline, TRUE))) {
  run_pipeline()
}
