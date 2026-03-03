# script: 31-export_lists.r
# description: export column-centric lists workbooks with one file per column
# and one sheet per layer.

#' @title Get fixed layer sheet order for lists exports
#' @description Returns deterministic sheet order for column-centric list
#' exports.
#' @return Character vector: `raw`, `clean`, `standardize`, `harmonize`.
get_lists_sheet_order <- function() {
  return(c("raw", "clean", "standardize", "harmonize"))
}

#' @title Map object name to layer sheet label
#' @description Infers the canonical sheet label from a layer object name.
#' @param object_name Character scalar object name.
#' @return Character scalar layer sheet label.
#' @importFrom checkmate assert_string
#' @importFrom cli cli_abort
infer_layer_sheet_name <- function(object_name) {
  checkmate::assert_string(object_name, min.chars = 1)

  if (grepl("_raw$", object_name)) {
    return("raw")
  }

  if (grepl("_cleaned$", object_name)) {
    return("clean")
  }

  if (grepl("_normalized$", object_name)) {
    return("standardize")
  }

  if (grepl("_harmonized$", object_name)) {
    return("harmonize")
  }

  cli::cli_abort(
    "unable to infer layer sheet name from object {.val {object_name}}"
  )
}

#' @title Build lists export path for one column
#' @description Resolves lists directory and returns a deterministic column-based
#' workbook path.
#' @param config Named configuration list.
#' @param column_name Character scalar column name.
#' @return Character scalar path ending with `_list.xlsx`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
build_column_lists_export_path <- function(config, column_name) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(column_name, min.chars = 1)

  lists_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "lists"),
    field_name = "config$paths$data$exports$lists"
  )

  lists_dir <- here::here(lists_dir)
  fs::dir_create(lists_dir, recurse = TRUE)

  return(fs::path(
    lists_dir,
    paste0("unique_", normalize_filename(column_name), "_list.xlsx")
  ))
}

#' @title Compute sorted unique values for one column
#' @description Returns sorted unique values when column exists; returns empty
#' character vector when the column is absent.
#' @param data_dt Data table for one layer.
#' @param column_name Character scalar column name.
#' @return Atomic vector of unique values.
#' @importFrom checkmate assert_data_table assert_string
#' @importFrom cli cli_abort
compute_unique_column_values <- function(data_dt, column_name) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_string(column_name, min.chars = 1)

  if (!column_name %in% names(data_dt)) {
    return(character(0))
  }

  column_values <- data_dt[[column_name]]

  if (is.list(column_values)) {
    cli::cli_abort(
      "column {.val {column_name}} has unsupported list type for list exports"
    )
  }

  unique_values <- unique(column_values)
  unique_values <- sort(unique_values, na.last = TRUE)

  return(unique_values)
}

#' @title Build layer tables keyed by sheet names
#' @description Creates deterministic layer table map keyed by
#' `raw/clean/standardize/harmonize`, filling missing layers with empty tables.
#' @param layer_tables Named list of detected layer data tables.
#' @return Named list of data.tables keyed by sheet name.
#' @importFrom checkmate assert_list
#' @importFrom data.table data.table
build_layer_tables_by_sheet <- function(layer_tables) {
  checkmate::assert_list(layer_tables, names = "named")

  layer_order <- get_lists_sheet_order()
  layer_by_sheet <- setNames(vector("list", length(layer_order)), layer_order)

  for (sheet_name in layer_order) {
    layer_by_sheet[[sheet_name]] <- data.table::data.table()
  }

  for (object_name in names(layer_tables)) {
    sheet_name <- infer_layer_sheet_name(object_name)

    if (ncol(layer_by_sheet[[sheet_name]]) == 0L && nrow(layer_by_sheet[[sheet_name]]) == 0L) {
      layer_by_sheet[[sheet_name]] <- data.table::as.data.table(layer_tables[[object_name]])
    }
  }

  return(layer_by_sheet)
}

#' @title Collect union of columns across all layer tables
#' @description Computes deterministic sorted union of column names from all
#' available layers.
#' @param layer_by_sheet Named list of data.tables keyed by sheet label.
#' @return Character vector of column names.
#' @importFrom checkmate assert_list
collect_union_columns <- function(layer_by_sheet) {
  checkmate::assert_list(layer_by_sheet, names = "named")

  union_columns <- unlist(
    lapply(layer_by_sheet, names),
    use.names = FALSE
  ) |>
    unique() |>
    sort()

  return(union_columns)
}

#' @title Build unique-values cache by layer and column
#' @description Precomputes unique values for every `(layer, column)` pair.
#' @param layer_by_sheet Named list of layer tables by sheet label.
#' @param union_columns Character vector of all columns.
#' @return Named list: first level sheet name, second level column name.
#' @importFrom checkmate assert_list assert_character
build_column_unique_cache <- function(layer_by_sheet, union_columns) {
  checkmate::assert_list(layer_by_sheet, names = "named")
  checkmate::assert_character(union_columns, min.len = 0, any.missing = FALSE)

  cache <- lapply(layer_by_sheet, function(layer_dt) {
    column_values <- lapply(union_columns, function(column_name) {
      compute_unique_column_values(layer_dt, column_name)
    })

    names(column_values) <- union_columns

    return(column_values)
  })

  return(cache)
}

#' @title Write one column-centric lists workbook
#' @description Writes one workbook per column with fixed ordered sheets:
#' `raw`, `clean`, `standardize`, `harmonize`.
#' @param column_name Character scalar column name.
#' @param unique_cache Named cache from `build_column_unique_cache()`.
#' @param config Named configuration list.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar workbook path.
#' @importFrom checkmate assert_string assert_list assert_flag
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom data.table data.table
write_column_lists_workbook <- function(
  column_name,
  unique_cache,
  config,
  overwrite = TRUE
) {
  checkmate::assert_string(column_name, min.chars = 1)
  checkmate::assert_list(unique_cache, names = "named")
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_flag(overwrite)

  workbook_path <- build_column_lists_export_path(
    config = config,
    column_name = column_name
  )

  workbook <- openxlsx::createWorkbook()
  sheet_order <- get_lists_sheet_order()

  for (sheet_name in sheet_order) {
    values <- unique_cache[[sheet_name]][[column_name]]

    if (is.null(values)) {
      values <- character(0)
    }

    values_dt <- data.table::data.table(value = values)

    openxlsx::addWorksheet(workbook, sheet_name)
    openxlsx::writeData(workbook, sheet_name, values_dt)
  }

  openxlsx::saveWorkbook(workbook, workbook_path, overwrite = overwrite)

  return(workbook_path)
}

#' @title Export column-centric lists workbooks
#' @description Exports one workbook per column. Each workbook contains fixed
#' ordered layer sheets (`raw`, `clean`, `standardize`, `harmonize`) with
#' sorted unique values for that column per layer.
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment for automatic object detection when `data_objects` is
#' `NULL`.
#' @return Named character vector of workbook paths keyed by column name.
#' @importFrom checkmate assert_list assert_flag assert_environment
#' @importFrom purrr map
#' @importFrom cli cli_abort
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

  layer_by_sheet <- build_layer_tables_by_sheet(layer_tables)
  union_columns <- collect_union_columns(layer_by_sheet)

  if (length(union_columns) == 0L) {
    cli::cli_abort("lists export failed: no columns found across detected layers")
  }

  unique_cache <- build_column_unique_cache(
    layer_by_sheet = layer_by_sheet,
    union_columns = union_columns
  )

  output_paths <- setNames(
    lapply(union_columns, function(column_name) {
      write_column_lists_workbook(
        column_name = column_name,
        unique_cache = unique_cache,
        config = config,
        overwrite = overwrite
      )
    }),
    union_columns
  )

  return(unlist(output_paths, use.names = TRUE))
}
