# script: 31-export_lists.r
# description: export column-centric lists workbooks with one file per column
# and one sheet per layer.

#' @title Get fixed layer sheet order for lists exports
#' @description Returns deterministic sheet order for column-centric list
#' exports.
#' @return Character vector: `raw`, `clean`, `harmonize`.
get_lists_sheet_order <- function() {
  return(c("raw", "clean", "harmonize"))
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
#' @importFrom fs path
build_column_lists_export_path <- function(config, column_name) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(column_name, min.chars = 1)

  lists_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "lists"),
    field_name = "config$paths$data$exports$lists"
  )

  lists_dir <- here::here(lists_dir)
  ensure_directories_exist(lists_dir, recurse = TRUE)

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
#' `raw/clean/harmonize`, filling missing layers with empty tables.
#' @param layer_tables Named list of detected layer data tables.
#' @return Named list of data.tables keyed by sheet name.
#' @importFrom checkmate assert_list
#' @importFrom data.table data.table
build_layer_tables_by_sheet <- function(layer_tables) {
  checkmate::assert_list(layer_tables, names = "named")

  layer_order <- get_lists_sheet_order()

  if (length(layer_tables) == 0) {
    layer_by_sheet <- lapply(layer_order, function(sheet_name) {
      data.table::data.table()
    })
    names(layer_by_sheet) <- layer_order

    return(layer_by_sheet)
  }

  detected_sheet_names <- vapply(
    names(layer_tables),
    infer_layer_sheet_name,
    character(1)
  )

  first_object_by_sheet <- tapply(
    names(layer_tables),
    detected_sheet_names,
    function(object_names) object_names[[1]],
    simplify = TRUE
  )

  layer_by_sheet <- lapply(layer_order, function(sheet_name) {
    selected_object <- first_object_by_sheet[[sheet_name]]

    if (is.null(selected_object) || is.na(selected_object)) {
      return(data.table::data.table())
    }

    return(ensure_data_table(layer_tables[[selected_object]]))
  })

  names(layer_by_sheet) <- layer_order

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

#' @title Normalize table for strict deterministic comparison
#' @description Drops `year` column when present, aligns column names and
#' order, and sorts rows so strict `identical()` can be applied deterministically.
#' @param data_dt Data table for normalization.
#' @return Normalized data.table.
#' @importFrom checkmate assert_data_table
normalize_for_comparison <- function(data_dt) {
  checkmate::assert_data_table(data_dt)

  normalized_dt <- data.table::copy(data_dt)

  if ("year" %in% names(normalized_dt)) {
    normalized_dt[, year := NULL]
  }

  normalized_columns <- sort(names(normalized_dt))

  if (length(normalized_columns) == 0L) {
    return(normalized_dt)
  }

  data.table::setcolorder(normalized_dt, normalized_columns)
  data.table::setorderv(normalized_dt, normalized_columns, na.last = TRUE)

  return(normalized_dt)
}

# backward-compatible alias
normalize_table_for_comparison <- normalize_for_comparison

#' @title Compare two list tables deterministically
#' @description Returns `TRUE` when two tables are strictly equal after
#' deterministic normalization.
#' @param left_dt Data table for left side.
#' @param right_dt Data table for right side.
#' @return Logical scalar.
#' @importFrom checkmate assert_data_table
are_list_tables_identical <- function(
  left_dt,
  right_dt
) {
  checkmate::assert_data_table(left_dt)
  checkmate::assert_data_table(right_dt)

  normalized_left <- normalize_for_comparison(left_dt)
  normalized_right <- normalize_for_comparison(right_dt)

  return(identical(normalized_left, normalized_right))
}

# backward-compatible alias
are_clean_harmonize_tables_identical <- are_list_tables_identical

#' @title Resolve deterministic sheet payloads for one column
#' @description Applies hierarchical equality logic across raw, clean, and
#' harmonize unique-value tables and returns the sheets that must be written.
#' @param raw_values_dt Data table of raw values.
#' @param clean_values_dt Data table of clean values.
#' @param harmonize_values_dt Data table of harmonize values.
#' @return Named list of sheet payload data.tables.
#' @importFrom checkmate assert_data_table
resolve_list_sheet_payloads <- function(
  raw_values_dt,
  clean_values_dt,
  harmonize_values_dt
) {
  checkmate::assert_data_table(raw_values_dt)
  checkmate::assert_data_table(clean_values_dt)
  checkmate::assert_data_table(harmonize_values_dt)

  raw_equals_clean <- are_list_tables_identical(raw_values_dt, clean_values_dt)
  raw_equals_harmonize <- are_list_tables_identical(
    raw_values_dt,
    harmonize_values_dt
  )
  clean_equals_harmonize <- are_list_tables_identical(
    clean_values_dt,
    harmonize_values_dt
  )

  if (raw_equals_clean && raw_equals_harmonize) {
    return(list(raw_clean_harmonize = raw_values_dt))
  }

  if (clean_equals_harmonize && !raw_equals_clean) {
    return(list(
      raw = raw_values_dt,
      clean_harmonize = clean_values_dt
    ))
  }

  return(list(
    raw = raw_values_dt,
    clean = clean_values_dt,
    harmonize = harmonize_values_dt
  ))
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
#' @description Writes one workbook per column with deterministic sheet logic:
#' all-equal lists produce `raw_clean_harmonize`; clean/harmonize equality with
#' raw difference produces `raw` + `clean_harmonize`; otherwise `raw`, `clean`,
#' and `harmonize` are written.
#' @param column_name Character scalar column name.
#' @param unique_cache Named cache from `build_column_unique_cache()`.
#' @param config Named configuration list.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar workbook path.
#' @importFrom checkmate assert_string assert_list assert_flag
#' @importFrom writexl write_xlsx
#' @importFrom data.table data.table
#' @importFrom cli cli_abort
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

  if (!overwrite && file.exists(workbook_path)) {
    cli::cli_abort(
      "file already exists and overwrite is disabled: {.path {workbook_path}}"
    )
  }

  raw_values <- unique_cache$raw[[column_name]]
  clean_values <- unique_cache$clean[[column_name]]
  harmonize_values <- unique_cache$harmonize[[column_name]]

  if (is.null(raw_values)) {
    raw_values <- character(0)
  }
  if (is.null(clean_values)) {
    clean_values <- character(0)
  }
  if (is.null(harmonize_values)) {
    harmonize_values <- character(0)
  }

  raw_values_dt <- data.table::data.table(value = raw_values)
  clean_values_dt <- data.table::data.table(value = clean_values)
  harmonize_values_dt <- data.table::data.table(value = harmonize_values)

  sheet_payloads <- resolve_list_sheet_payloads(
    raw_values_dt = raw_values_dt,
    clean_values_dt = clean_values_dt,
    harmonize_values_dt = harmonize_values_dt
  )

  writexl::write_xlsx(sheet_payloads, path = workbook_path, col_names = FALSE)

  return(workbook_path)
}

#' @title Export column-centric lists workbooks
#' @description Exports one workbook per column. Each workbook contains fixed
#' deterministic layer sheet outputs: always `raw`, plus either a merged
#' `clean_harmonize` sheet or separate `clean` and `harmonize` sheets.
#' The `value` column workbook is excluded from list exports. When a `future`
#' parallel backend is configured, workbooks are written in parallel.
#' @param config Named configuration list.
#' @param data_objects Optional named list of data.frame/data.table objects.
#' @param overwrite Logical scalar overwrite flag.
#' @param env Environment for automatic object detection when `data_objects` is
#' `NULL`.
#' @return Named character vector of workbook paths keyed by column name.
#' @importFrom checkmate assert_list assert_flag assert_environment
#' @importFrom future.apply future_lapply
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
    cli::cli_abort(
      "lists export failed: no columns found across detected layers"
    )
  }

  unique_cache <- build_column_unique_cache(
    layer_by_sheet = layer_by_sheet,
    union_columns = union_columns
  )

  export_columns <- union_columns[
    !union_columns %in% c("value", "year", "yearbook")
  ]

  use_parallel <- !inherits(future::plan(), "sequential") &&
    length(export_columns) > 1L

  if (use_parallel) {
    output_paths <- setNames(
      future.apply::future_lapply(
        export_columns,
        function(column_name) {
          write_column_lists_workbook(
            column_name = column_name,
            unique_cache = unique_cache,
            config = config,
            overwrite = overwrite
          )
        },
        future.seed = NULL
      ),
      export_columns
    )
  } else {
    output_paths <- setNames(
      lapply(export_columns, function(column_name) {
        write_column_lists_workbook(
          column_name = column_name,
          unique_cache = unique_cache,
          config = config,
          overwrite = overwrite
        )
      }),
      export_columns
    )
  }

  return(unlist(output_paths, use.names = TRUE))
}
