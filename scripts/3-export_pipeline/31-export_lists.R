# script: 31-export_lists.r
# description: export per-table unique-value lists into deterministic list
# workbooks.

#' @title Build lists export path for an object
#' @description Resolves the lists export directory from config, ensures the
#' directory exists, and returns an object-name-based lists workbook path.
#' @param config Named configuration list with `paths$data$exports$lists`.
#' @param object_name Character scalar object name.
#' @return Character scalar path ending with `_lists.xlsx`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
build_lists_export_path <- function(config, object_name) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(object_name, min.chars = 1)

  lists_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "lists"),
    field_name = "config$paths$data$exports$lists"
  )

  lists_dir <- here::here(lists_dir)
  fs::dir_create(lists_dir, recurse = TRUE)

  return(fs::path(
    lists_dir,
    paste0(normalize_filename(object_name), "_lists.xlsx")
  ))
}

#' @title Normalize sheet name
#' @description Normalizes worksheet names to Excel-safe identifiers.
#' @param col_name Atomic vector of sheet labels.
#' @return Character vector of normalized sheet names.
#' @importFrom checkmate check_atomic_vector
#' @importFrom stringr str_sub
normalize_sheet_name <- function(col_name) {
  assert_or_abort(checkmate::check_atomic_vector(
    col_name,
    min.len = 1,
    any.missing = TRUE
  ))

  sheet_name <- col_name |>
    as.character() |>
    normalize_filename() |>
    stringr::str_sub(1, 31)

  sheet_name[is.na(sheet_name) | sheet_name == ""] <- "unknown"

  return(sheet_name)
}

#' @title Build unique-column lists for one table
#' @description Computes sorted unique values for each column in a data table.
#' @param data_dt Data table.
#' @return Named list where each element is a data.table with one column.
#' @importFrom checkmate assert_data_table
#' @importFrom data.table data.table
build_unique_lists_by_column <- function(data_dt) {
  checkmate::assert_data_table(data_dt)

  column_names <- names(data_dt)

  unique_lists <- lapply(column_names, function(column_name) {
    values <- data_dt[[column_name]]
    unique_values <- unique(values)

    if (is.character(unique_values)) {
      unique_values <- sort(unique_values, na.last = TRUE)
    } else {
      unique_values <- sort(unique_values, na.last = TRUE)
    }

    data.table::data.table(value = unique_values)
  })

  names(unique_lists) <- column_names

  return(unique_lists)
}

#' @title Write one table lists workbook
#' @description Writes one workbook with one sheet per column containing sorted
#' unique values.
#' @param data_dt Data table.
#' @param object_name Character scalar source object name.
#' @param config Named configuration list.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar path of written workbook.
#' @importFrom checkmate assert_data_table assert_string assert_list assert_flag
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
write_table_lists_workbook <- function(
  data_dt,
  object_name,
  config,
  overwrite = TRUE
) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_string(object_name, min.chars = 1)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_flag(overwrite)

  workbook_path <- build_lists_export_path(config = config, object_name = object_name)
  unique_lists <- build_unique_lists_by_column(data_dt)

  workbook <- openxlsx::createWorkbook()

  purrr::iwalk(unique_lists, function(list_dt, column_name) {
    sheet_name <- normalize_sheet_name(column_name)
    openxlsx::addWorksheet(workbook, sheet_name)
    openxlsx::writeData(workbook, sheet_name, list_dt)
  })

  openxlsx::saveWorkbook(workbook, workbook_path, overwrite = overwrite)

  return(workbook_path)
}

#' @title Export lists workbooks for layer tables
#' @description Exports one lists workbook per detected layer table.
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment for automatic object detection when `data_objects` is
#' `NULL`.
#' @return Named character vector of list workbook paths.
#' @importFrom checkmate assert_list assert_flag assert_environment
#' @importFrom purrr imap_chr
export_lists <- function(
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

  list_paths <- purrr::imap_chr(layer_tables, function(data_dt, object_name) {
    write_table_lists_workbook(
      data_dt = data_dt,
      object_name = object_name,
      config = config,
      overwrite = overwrite
    )
  })

  return(list_paths)
}
