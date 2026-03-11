# script: 30-export_data.r
# description: detect post-processing layer tables and export the harmonized
# layer as a deterministic processed-data workbook using a high-performance
# C-based writer.

#' @title Build processed-data export path for an object
#' @description Resolves the processed export directory from config and returns
#' an object-name-based workbook path. Callers must ensure the directory exists
#' before writing (see `export_processed_data`).
#' @param config Named configuration list with
#' `paths$data$exports$processed`.
#' @param object_name Character scalar object name.
#' @return Character scalar path ending with `.xlsx`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs path
build_processed_export_path <- function(config, object_name) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(object_name, min.chars = 1)

  processed_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "processed"),
    field_name = "config$paths$data$exports$processed"
  )

  processed_dir <- here::here(processed_dir)

  return(fs::path(processed_dir, paste0(normalize_filename(object_name), ".xlsx")))
}


#' @title Detect available layer tables for export
#' @description Discovers available data.frame/data.table objects that end with
#' configured layer suffixes.
#' @param data_objects Optional named list of data objects.
#' @param env Environment used for automatic detection when `data_objects` is
#' `NULL`.
#' @param layer_suffixes Character vector of supported layer suffixes.
#' @return Named list of data.table objects keyed by original object names.
#' @importFrom checkmate assert_environment assert_character assert_list
#' @importFrom cli cli_abort
#' @importFrom data.table as.data.table
#' @importFrom purrr keep map
collect_layer_tables_for_export <- function(
  data_objects = NULL,
  env = .GlobalEnv,
  layer_suffixes = c("raw", "cleaned", "normalized", "harmonized")
) {
  checkmate::assert_environment(env)
  checkmate::assert_character(layer_suffixes, min.len = 1, any.missing = FALSE, unique = TRUE)

  layer_pattern <- paste0("_(", paste(layer_suffixes, collapse = "|"), ")$")

  is_valid_layer_name <- function(object_name) {
    return(
      !is.na(object_name) &&
        nzchar(object_name) &&
        grepl(layer_pattern, object_name) &&
        !grepl("_post_processed$", object_name) &&
        !grepl("_wide_raw$", object_name)
    )
  }

  if (is.null(data_objects)) {
    candidate_names <- ls(envir = env, all.names = TRUE)
    valid_candidate_names <- Filter(is_valid_layer_name, candidate_names)

    detected_tables <- purrr::keep(
      setNames(lapply(valid_candidate_names, get, envir = env, inherits = TRUE), valid_candidate_names),
      is.data.frame
    )
  } else {
    checkmate::assert_list(data_objects, names = "named", any.missing = TRUE)

    object_names <- names(data_objects)
    valid_name_mask <- vapply(object_names, is_valid_layer_name, logical(1))

    detected_tables <- data_objects[valid_name_mask]
    detected_tables <- purrr::keep(detected_tables, is.data.frame)
  }

  if (length(detected_tables) == 0L) {
    cli::cli_abort(c(
      "no layer tables detected for export.",
      "x" = "expected object names ending in: {.val {layer_suffixes}}",
      "i" = "excluded suffixes include {.val _post_processed} and {.val _wide_raw}"
    ))
  }

  detected_tables <- detected_tables[sort(names(detected_tables))]

  detected_tables <- detected_tables[
    !grepl("_post_processed$", names(detected_tables))
  ]

  ordered_names <- sort(names(detected_tables))
  detected_tables <- detected_tables[ordered_names]

  return(purrr::map(detected_tables, data.table::as.data.table))
}

#' @title Write one data table to Excel (high-performance)
#' @description Writes a data.table to a single-sheet `.xlsx` file using the
#' C-based `writexl` engine.
#' @param data_dt Data table to export.
#' @param output_path Character scalar path to write.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar `output_path`.
#' @importFrom checkmate assert_data_table assert_string assert_flag
#' @importFrom writexl write_xlsx
write_processed_table_fast <- function(data_dt, output_path, overwrite = TRUE) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_string(output_path, min.chars = 1)
  checkmate::assert_flag(overwrite)

  if (!overwrite && file.exists(output_path)) {
    cli::cli_abort("file already exists and overwrite is disabled: {.path {output_path}}")
  }

  writexl::write_xlsx(data_dt, path = output_path)

  return(output_path)
}

#' @title Export processed layer tables
#' @description Detects all layer tables for traceability, then exports only the
#' layers listed in `config$export_config$export_layers` (default:
#' `"harmonized"`) into `data/3-export/processed_data` using the
#' high-performance writer. Callers must ensure the processed-data export
#' directory exists before calling this function (see `run_export_pipeline`).
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment for automatic object detection when `data_objects` is
#' `NULL`.
#' @return Named character vector of processed export paths.
#' @importFrom checkmate assert_list assert_flag assert_environment assert_character
#' @importFrom purrr imap_chr
export_processed_data <- function(
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

  export_layers <- config$export_config$export_layers
  if (is.null(export_layers)) {
    export_layers <- c("harmonized")
  }
  checkmate::assert_character(export_layers, min.len = 1, any.missing = FALSE)

  export_pattern <- paste0("_(", paste(export_layers, collapse = "|"), ")$")
  export_tables <- layer_tables[grepl(export_pattern, names(layer_tables))]

  if (length(export_tables) == 0L) {
    cli::cli_abort(c(
      "no exportable layer tables found.",
      "i" = "detected layers: {.val {names(layer_tables)}}",
      "x" = "config$export_config$export_layers is set to: {.val {export_layers}}"
    ))
  }

  processed_paths <- purrr::imap_chr(export_tables, function(data_dt, object_name) {
    output_path <- build_processed_export_path(config = config, object_name = object_name)

    write_processed_table_fast(
      data_dt = data_dt,
      output_path = output_path,
      overwrite = overwrite
    )
  })

  return(processed_paths)
}
