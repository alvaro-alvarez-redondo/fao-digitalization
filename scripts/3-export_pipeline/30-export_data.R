# script: 30-export_data.r
# description: detect post-processing layer tables and export each as a
# deterministic processed-data workbook.

#' @title Build processed-data export path for an object
#' @description Resolves the processed export directory from config, ensures the
#' directory exists, and returns an object-name-based workbook path.
#' @param config Named configuration list with
#' `paths$data$exports$processed`.
#' @param object_name Character scalar object name.
#' @return Character scalar path ending with `.xlsx`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
build_processed_export_path <- function(config, object_name) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(object_name, min.chars = 1)

  processed_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "processed"),
    field_name = "config$paths$data$exports$processed"
  )

  processed_dir <- here::here(processed_dir)
  fs::dir_create(processed_dir, recurse = TRUE)

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
#' @importFrom checkmate assert_environment assert_character
#' @importFrom cli cli_abort
#' @importFrom data.table as.data.table
#' @importFrom purrr keep map
collect_layer_tables_for_export <- function(
  data_objects = NULL,
  env = .GlobalEnv,
  layer_suffixes = c("raw", "clean", "standardize", "harmonize")
) {
  checkmate::assert_environment(env)
  checkmate::assert_character(layer_suffixes, min.len = 1, any.missing = FALSE)

  layer_pattern <- paste0("_(", paste(layer_suffixes, collapse = "|"), ")$")

  if (is.null(data_objects)) {
    candidate_names <- ls(envir = env, all.names = TRUE)
    candidate_names <- candidate_names[grepl(layer_pattern, candidate_names)]

    detected_tables <- purrr::keep(
      setNames(lapply(candidate_names, get, envir = env, inherits = TRUE), candidate_names),
      is.data.frame
    )
  } else {
    checkmate::assert_list(data_objects, names = "named")

    object_names <- names(data_objects)
    valid_name_mask <- !is.na(object_names) &
      nzchar(object_names) &
      grepl(layer_pattern, object_names)

    detected_tables <- data_objects[valid_name_mask]
    detected_tables <- purrr::keep(detected_tables, is.data.frame)
  }

  if (length(detected_tables) == 0L) {
    cli::cli_abort(
      "no layer tables detected for export. expected names ending in: {.val {layer_suffixes}}"
    )
  }

  ordered_names <- sort(names(detected_tables))
  detected_tables <- detected_tables[ordered_names]

  return(purrr::map(detected_tables, data.table::as.data.table))
}

#' @title Write one data table to Excel
#' @description Writes a data.table to a single-sheet workbook with fail-fast
#' semantics.
#' @param data_dt Data table to export.
#' @param output_path Character scalar path to write.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar `output_path`.
#' @importFrom checkmate assert_data_table assert_string assert_flag
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
write_processed_table_excel <- function(data_dt, output_path, overwrite = TRUE) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_string(output_path, min.chars = 1)
  checkmate::assert_flag(overwrite)

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "data")
  openxlsx::writeData(workbook, "data", data_dt)
  openxlsx::saveWorkbook(workbook, output_path, overwrite = overwrite)

  return(output_path)
}

#' @title Export processed layer tables
#' @description Exports every detected layer table into
#' `data/3-export/processed_data` as one workbook per object.
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment for automatic object detection when `data_objects` is
#' `NULL`.
#' @return Named character vector of processed export paths.
#' @importFrom checkmate assert_list assert_flag assert_environment
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

  processed_paths <- purrr::imap_chr(layer_tables, function(data_dt, object_name) {
    output_path <- build_processed_export_path(config = config, object_name = object_name)

    write_processed_table_excel(
      data_dt = data_dt,
      output_path = output_path,
      overwrite = overwrite
    )
  })

  return(processed_paths)
}
