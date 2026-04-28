# script: run_export_pipeline.r
# description: source export components and run deterministic per-layer export
# for processed data and lists workbooks.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(
    here::here("r", "0-general_pipeline", "01-setup.R"),
    echo = FALSE
  )
}


export_scripts <- c(
  "30-export_data.R",
  "31-export_lists.R",
  "32-export_anomaly_bundle.R"
)

purrr::walk(
  export_scripts,
  \(script_name) {
    source(
      here::here("r", "4-export_pipeline", script_name),
      echo = FALSE
    )
  }
)

#' @title Run export pipeline
#' @description Detects available layer data tables and export per-layer
#' processed-data and lists workbooks.
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment used for automatic object detection when
#' `data_objects` is `NULL`.
#' @return Named list with `processed_paths` and `lists_paths` as named
#' character vectors.
#' @importFrom checkmate assert_list assert_flag assert_environment
#' @importFrom progressr handlers handler_txtprogressbar with_progress progressor
run_export_pipeline <- function(
  config,
  data_objects = NULL,
  overwrite = TRUE,
  env = .GlobalEnv
) {
  checkmate::assert_list(config, names = "named")
  checkmate::assert_flag(overwrite)
  checkmate::assert_environment(env)

  layer_tables <- collect_layer_tables_for_export(
    data_objects = data_objects,
    env = env
  )

  # pre-flight: create all export directories once so that no downstream
  # function inside a loop needs to call ensure_directories_exist / fs::dir_create
  processed_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "export", "processed"),
    field_name = "config$paths$data$export$processed"
  )
  lists_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "export", "lists"),
    field_name = "config$paths$data$export$lists"
  )
  ensure_directories_exist(
    c(here::here(processed_dir), here::here(lists_dir)),
    recurse = TRUE
  )

  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    width = 40,
    clear = FALSE
  ))

  export_result <- progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(3L))

    processed_paths <- export_processed_data(
      config = config,
      data_objects = layer_tables,
      overwrite = overwrite,
      env = env
    )
    progress("export pipeline: processed workbooks")

    lists_paths <- export_lists(
      config = config,
      data_objects = layer_tables,
      overwrite = overwrite,
      env = env
    )
    progress("export pipeline: lists workbooks")

    anomaly_bundle <- collect_anomaly_bundle_for_export(env = env)
    assert_anomaly_bundle_export_contract(anomaly_bundle)

    anomaly_export_path <- export_anomaly_bundle(
      config = config,
      anomaly_bundle = anomaly_bundle,
      overwrite = overwrite
    )
    progress("export pipeline: anomaly workbook")

    return(list(
      processed_paths = processed_paths,
      lists_paths = lists_paths,
      anomaly_path = anomaly_export_path
    ))
  })

  assert_export_paths_contract(export_result)

  return(export_result)
}

#' @title Assert export paths contract
#' @description Validates stable return contract for `run_export_pipeline()`.
#' @param export_result Named list expected to include `processed_paths` and
#' `lists_paths` as named character vectors.
#' @return Invisible `TRUE` when contract is valid.
#' @importFrom checkmate assert_list assert_names assert_character
assert_export_paths_contract <- function(export_result) {
  checkmate::assert_list(export_result, any.missing = FALSE)
  checkmate::assert_names(
    names(export_result),
    must.include = c("processed_paths", "lists_paths", "anomaly_path")
  )
  checkmate::assert_character(
    export_result$processed_paths,
    min.len = 1,
    names = "named"
  )
  checkmate::assert_character(
    export_result$lists_paths,
    min.len = 1,
    names = "named"
  )
  checkmate::assert_string(export_result$anomaly_path, min.chars = 1)

  return(invisible(TRUE))
}

#' @title Run export pipeline automatically
#' @description Executes `run_export_pipeline()` when automatic mode is enabled.
#' @param auto_run Logical scalar controlling automatic execution.
#' @param env Environment used to resolve and assign stage objects.
#' @return Invisible export-paths list when executed; otherwise invisible `NULL`.
#' @importFrom checkmate assert_flag assert_environment
#' @importFrom cli cli_warn
run_export_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  pipeline_constants <- get_pipeline_constants()

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("config", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic export pipeline skipped: missing {.val config} in environment"
    )
    return(invisible(NULL))
  }

  config_value <- get("config", envir = env, inherits = TRUE)

  export_paths <- run_export_pipeline(
    config = config_value,
    data_objects = NULL,
    overwrite = TRUE,
    env = env
  )

  assignment_helper <- pipeline_constants$helper_requirements$assignment_helper

  if (!exists(assignment_helper, mode = "function", inherits = TRUE)) {
    cli::cli_abort(
      "missing shared helper {.fn {assignment_helper}}; source {.file {pipeline_constants$helper_requirements$assignment_helper_source}}"
    )
  }

  assignment_values <- list(export_paths)
  names(assignment_values) <- pipeline_constants$object_names$export_paths

  assign_environment_values(values = assignment_values, env = env)

  return(invisible(export_paths))
}

run_export_pipeline_auto(
  auto_run = isTRUE(getOption(
    get_pipeline_constants()$auto_run_options$export,
    TRUE
  ))
)
