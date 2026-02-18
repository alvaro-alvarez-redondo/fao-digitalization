# script: data audit script
# description: validate consolidated import data, isolate invalid records,
# and export audit artifacts with mirrored raw source files.

#' @title load audit configuration
#' @description validate required audit configuration fields used by the import
#' audit pipeline. this function validates required columns, optional
#' `audit_columns_by_type`, and required input/output paths.
#' @param config named list containing at minimum `column_order`,
#' `audit_columns`, and required path fields under `paths$data`.
#' @return invisible `TRUE` when validation succeeds.
#' @examples
#' config_example <- load_pipeline_config("fao_data_raw")
#' load_audit_config(config_example)
load_audit_config <- function(config) {
  assert_or_abort(checkmate::check_list(
    config,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_character(
    config$column_order,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_character(
    config$audit_columns,
    min.len = 1,
    any.missing = FALSE
  ))

  if (!is.null(config$audit_columns_by_type)) {
    assert_or_abort(checkmate::check_list(
      config$audit_columns_by_type,
      min.len = 1,
      any.missing = FALSE
    ))
    purrr::walk(config$audit_columns_by_type, function(audit_columns) {
      assert_or_abort(checkmate::check_character(
        audit_columns,
        min.len = 1,
        any.missing = FALSE
      ))
    })
  }

  assert_or_abort(checkmate::check_string(
    config$paths$data$imports$raw,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_string(
    config$paths$data$audit$audit_file_path,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_string(
    config$paths$data$audit$raw_imports_mirror_dir,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_list(
    config$export_config$styles,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_list(
    config$export_config$styles$error_highlight,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_string(
    config$export_config$styles$error_highlight$fgFill,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_string(
    config$export_config$styles$error_highlight$fontColour,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_string(
    config$export_config$styles$error_highlight$textDecoration,
    min.chars = 1
  ))

  invisible(TRUE)
}

#' @title resolve audit columns by validation type
#' @description return audit columns grouped by validator type. if
#' `config$audit_columns_by_type` is present, it is returned. otherwise,
#' a default mapping is created from `config$audit_columns` and
#' `config$column_order`.
#' @param config named list with audit configuration.
#' @return named list where each name is an audit type and each value is a
#' character vector of column names.
#' @examples
#' config_example <- list(
#'   column_order = c("document", "value"),
#'   audit_columns = c("document", "value"),
#'   paths = list(
#'     data = list(
#'       imports = list(raw = "data/imports/raw"),
#'       audit = list(
#'         audit_file_path = "data/audit/audit.xlsx",
#'         raw_imports_mirror_dir = "data/audit/mirror"
#'       )
#'     )
#'   )
#' )
#' # resolve_audit_columns_by_type(config_example)
resolve_audit_columns_by_type <- function(config) {
  assert_or_abort(checkmate::check_list(
    config,
    min.len = 1,
    any.missing = FALSE
  ))
  load_audit_config(config)

  if (!is.null(config$audit_columns_by_type)) {
    audit_columns_by_type <- config$audit_columns_by_type
  } else {
    audit_columns_by_type <- list(
      character_non_empty = unique(config$audit_columns),
      numeric_string = intersect("value", config$column_order)
    )
  }
  audit_columns_by_type
}

#' @title audit non-empty character values
#' @description validate that all values in `column_name` are non-missing and
#' non-empty after trimming whitespace.
#' @param dataset_dt data frame or data table to validate.
#' @param column_name character scalar naming the target column.
#' @return data table with standardized audit findings columns:
#' `row_index`, `audit_column`, `audit_type`, and `audit_message`.
#' @examples
#' dataset_example <- data.frame(document = c("a.xlsx", " "))
#' audit_character_non_empty(dataset_example, "document")
audit_character_non_empty <- function(dataset_dt, column_name) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(
    names(dataset_dt),
    must.include = column_name
  ))
  assert_or_abort(checkmate::check_character(
    dataset_dt[[column_name]],
    any.missing = TRUE,
    null.ok = FALSE
  ))

  column_values <- dataset_dt[[column_name]]
  invalid_rows <- which(is.na(column_values) | !nzchar(trimws(column_values)))

  if (length(invalid_rows) == 0) {
    return(empty_audit_findings_dt())
  }

  data.table::data.table(
    row_index = invalid_rows,
    audit_column = column_name,
    audit_type = "character_non_empty",
    audit_message = "value must be a non-empty character string"
  )
}

#' @title audit numeric string values
#' @description validate that all non-missing values in `column_name` match
#' a numeric string pattern with optional decimal point.
#' @param dataset_dt data frame or data table to validate.
#' @param column_name character scalar naming the target column. defaults to
#' `"value"`.
#' @return data table with standardized audit findings columns:
#' `row_index`, `audit_column`, `audit_type`, and `audit_message`.
#' @examples
#' dataset_example <- data.frame(value = c("10", "1.5", "x"))
#' audit_numeric_string(dataset_example)
audit_numeric_string <- function(dataset_dt, column_name = "value") {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(
    names(dataset_dt),
    must.include = column_name
  ))
  assert_or_abort(checkmate::check_atomic(
    dataset_dt[[column_name]],
    any.missing = TRUE
  ))

  column_values <- as.character(dataset_dt[[column_name]])
  invalid_rows <- which(
    !is.na(column_values) & !grepl("^[0-9]+(\\.[0-9]+)?$", column_values)
  )

  if (length(invalid_rows) == 0) {
    return(empty_audit_findings_dt())
  }

  data.table::data.table(
    row_index = invalid_rows,
    audit_column = column_name,
    audit_type = "numeric_string",
    audit_message = "value must contain only digits and at most one decimal point"
  )
}

#' @title run master validation
#' @description execute all configured validators over selected columns and
#' return combined audit findings with unique invalid row indexes.
#' @param dataset_dt data frame or data table to validate.
#' @param audit_columns_by_type named list mapping audit types to character
#' vectors of column names.
#' @param selected_validations optional character vector with audit types to
#' run. defaults to `NULL` for all configured validations.
#' @return named list with:
#' \itemize{
#'   \item `findings`: data table of all validation findings.
#'   \item `invalid_row_index`: integer vector of unique invalid row indexes.
#' }
#' @examples
#' dataset_example <- data.frame(document = c("a.xlsx", ""), value = c("1", "x"))
#' audit_map <- list(
#'   character_non_empty = "document",
#'   numeric_string = "value"
#' )
#' run_master_validation(dataset_example, audit_map)
run_master_validation <- function(
  dataset_dt,
  audit_columns_by_type,
  selected_validations = NULL
) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(
    audit_columns_by_type,
    min.len = 1,
    any.missing = FALSE
  ))

  validator_registry <- list(
    character_non_empty = audit_character_non_empty,
    numeric_string = audit_numeric_string
  )

  if (!is.null(selected_validations)) {
    assert_or_abort(checkmate::check_character(
      selected_validations,
      min.len = 1,
      any.missing = FALSE,
      unique = TRUE
    ))
    audit_columns_by_type <- audit_columns_by_type[
      names(audit_columns_by_type) %in% selected_validations
    ]
  }

  audit_types <- names(audit_columns_by_type)
  unsupported_audit_types <- setdiff(audit_types, names(validator_registry))
  if (length(unsupported_audit_types) > 0) {
    cli::cli_warn(c(
      "unsupported audit types were skipped",
      "i" = "audit types: {toString(unsupported_audit_types)}"
    ))
  }

  supported_audit_types <- intersect(audit_types, names(validator_registry))
  findings_list <- purrr::map(supported_audit_types, function(audit_type) {
    column_names <- audit_columns_by_type[[audit_type]]
    purrr::map(
      column_names,
      function(column_name) {
        validator_registry[[audit_type]](
          dataset_dt = dataset_dt,
          column_name = column_name
        )
      }
    )
  }) |>
    purrr::flatten()

  findings_dt <- data.table::rbindlist(findings_list, fill = TRUE)
  if (nrow(findings_dt) == 0) {
    findings_dt <- empty_audit_findings_dt()
  }

  list(
    findings = findings_dt,
    invalid_row_index = sort(unique(findings_dt$row_index))
  )
}

#' @title run audit by type
#' @description backward-compatible wrapper that resolves validation map from
#' configuration and delegates execution to `run_master_validation()`.
#' @param dataset_dt data frame or data table to validate.
#' @param config named audit configuration list.
#' @return named list returned by `run_master_validation()`.
#' @examples
#' dataset_example <- data.frame(document = "a.xlsx", value = "1")
#' config_example <- list(
#'   column_order = c("document", "value"),
#'   audit_columns = c("document", "value"),
#'   paths = list(
#'     data = list(
#'       imports = list(raw = "data/imports/raw"),
#'       audit = list(
#'         audit_file_path = "data/audit/audit.xlsx",
#'         raw_imports_mirror_dir = "data/audit/mirror"
#'       )
#'     )
#'   )
#' )
#' # run_audit_by_type(dataset_example, config_example)
run_audit_by_type <- function(dataset_dt, config) {
  audit_columns_by_type <- resolve_audit_columns_by_type(config)
  run_master_validation(dataset_dt, audit_columns_by_type)
}

#' @title identify audit errors
#' @description run configured audits against `fao_data_raw` and return only
#' invalid records sorted by `document`.
#' @param fao_data_raw data frame or data table with consolidated raw import
#' data.
#' @param config named audit configuration list.
#' @param include_findings logical scalar. when `true`, return a named list with
#' `audit_dt` and `findings_dt`.
#' @return data table containing unique invalid rows when
#' `include_findings = false`; otherwise a named list with `audit_dt` joined to
#' per-cell findings and `findings_dt`.
#' @examples
#' # identify_audit_errors(fao_data_raw, config)
identify_audit_errors <- function(
  fao_data_raw,
  config,
  include_findings = FALSE
) {
  assert_or_abort(checkmate::check_data_frame(fao_data_raw, min.rows = 0))
  assert_or_abort(checkmate::check_list(
    config,
    any.missing = FALSE,
    min.len = 1
  ))
  assert_or_abort(checkmate::check_flag(include_findings))
  load_audit_config(config)
  assert_or_abort(checkmate::check_names(
    names(fao_data_raw),
    must.include = config$column_order
  ))

  audit_result <- run_audit_by_type(dataset_dt = fao_data_raw, config = config)
  findings_dt <- data.table::as.data.table(audit_result$findings)

  if (nrow(findings_dt) == 0) {
    output_dt <- data.table::as.data.table(fao_data_raw)[0]

    if (include_findings) {
      return(list(
        audit_dt = output_dt,
        findings_dt = empty_audit_findings_dt()
      ))
    }

    return(output_dt)
  }

  source_dt <- data.table::as.data.table(fao_data_raw)
  detailed_output_dt <- cbind(
    data.table::copy(findings_dt),
    data.table::copy(source_dt[findings_dt$row_index])
  )

  if ("document" %in% names(detailed_output_dt)) {
    data.table::setorderv(
      detailed_output_dt,
      cols = c("document", "row_index"),
      na.last = TRUE
    )
  } else {
    data.table::setorderv(
      detailed_output_dt,
      cols = "row_index",
      na.last = TRUE
    )
  }

  if (include_findings) {
    return(list(audit_dt = detailed_output_dt, findings_dt = findings_dt))
  }

  output_dt <- data.table::copy(source_dt[audit_result$invalid_row_index])
  if ("document" %in% names(output_dt)) {
    data.table::setorderv(output_dt, cols = "document", na.last = TRUE)
  }

  output_dt
}

#' @title mirror raw import errors
#' @description copy raw import files associated with invalid audit records into
#' a mirror directory while preserving relative folder structure.
#' @param audit_dt data frame or data table containing at least `document`.
#' @param raw_imports_dir character scalar path to raw import files.
#' @param raw_imports_mirror_dir character scalar path to mirror destination.
#' @return invisibly returns character vector of mirrored target file paths.
#' @examples
#' # mirror_raw_import_errors(audit_dt, "data/imports/raw", "data/audit/mirror")
mirror_raw_import_errors <- function(
  audit_dt,
  raw_imports_dir,
  raw_imports_mirror_dir
) {
  assert_or_abort(checkmate::check_data_frame(audit_dt, min.rows = 0))
  assert_or_abort(checkmate::check_names(
    names(audit_dt),
    must.include = "document"
  ))
  assert_or_abort(checkmate::check_string(raw_imports_dir, min.chars = 1))
  assert_or_abort(checkmate::check_string(
    raw_imports_mirror_dir,
    min.chars = 1
  ))
  assert_or_abort(checkmate::check_directory_exists(raw_imports_dir))

  error_documents <- unique(as.character(audit_dt$document))
  error_documents <- error_documents[
    !is.na(error_documents) & nzchar(error_documents)
  ]
  if (length(error_documents) == 0) {
    return(invisible(character(0)))
  }

  # clear the mirror directory before copying.
  if (fs::dir_exists(raw_imports_mirror_dir)) {
    fs::dir_delete(raw_imports_mirror_dir)
  }
  fs::dir_create(raw_imports_mirror_dir)

  raw_files <- fs::dir_ls(
    path = raw_imports_dir,
    type = "file",
    recurse = TRUE,
    glob = "*.xlsx"
  )
  if (length(raw_files) == 0) {
    cli::cli_warn("no raw import files found under {.path {raw_imports_dir}}")
    return(invisible(character(0)))
  }

  raw_file_names <- fs::path_file(raw_files)
  selected_rows <- raw_file_names %in% error_documents
  if (!any(selected_rows)) {
    cli::cli_warn(
      "no matching raw import files were found for mirrored audit output"
    )
    return(invisible(character(0)))
  }

  unmatched_documents <- setdiff(error_documents, raw_file_names)
  if (length(unmatched_documents) > 0) {
    cli::cli_warn(c(
      "some audited documents were not found in raw imports",
      "i" = "missing files: {toString(unmatched_documents)}"
    ))
  }

  matched_paths <- raw_files[selected_rows]
  relative_paths <- fs::path_rel(matched_paths, start = raw_imports_dir)
  target_paths <- fs::path(raw_imports_mirror_dir, relative_paths)
  fs::dir_create(fs::path_dir(target_paths))
  fs::file_copy(matched_paths, target_paths, overwrite = TRUE)

  invisible(as.character(target_paths))
}

#' @title export validation audit report
#' @description write audit results to an excel workbook at `output_path`.
#' output is sorted by `document` and written to sheet `audit_report`.
#' @param audit_dt data frame or data table containing at least `document`.
#' @param config named audit configuration list containing
#' `export_config$styles$error_highlight`.
#' @param findings_dt data table with at least `row_index` and `audit_column`.
#' if `null`, findings are inferred from `audit_dt` when available.
#' @param output_path character scalar destination path for the excel file.
#' @return character scalar with written output path.
#' @examples
#' # export_validation_audit_report(audit_dt, config)
#' @title export validation audit report
#' @description write audit results to an excel workbook at `output_path`.
#' output is sorted by `document` and written to sheet `audit_report` with
#' specific cells highlighted based on config styles.
export_validation_audit_report <- function(
  audit_dt,
  config,
  findings_dt = NULL,
  output_path = config$paths$data$audit$audit_file_path
) {
  assert_or_abort(checkmate::check_data_frame(audit_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(
    config,
    min.len = 1,
    any.missing = FALSE
  ))
  load_audit_config(config) # Valida que existan los estilos

  # 1. Preparar datos de exportación (Sin borrar columnas aún)
  export_dt <- data.table::as.data.table(data.table::copy(audit_dt))

  if ("row_index" %in% names(export_dt)) {
    export_dt[, source_row_index := row_index]
  } else {
    export_dt[, source_row_index := seq_len(.N)]
  }

  # Ordenar por documento ANTES de calcular la posición de Excel
  data.table::setorderv(export_dt, cols = "document", na.last = TRUE)

  # 2. Definir columnas técnicas a excluir del Excel final
  technical_cols <- c(
    "source_row_index",
    "row_index",
    "audit_column",
    "audit_type",
    "audit_message"
  )
  cols_to_show <- setdiff(names(export_dt), technical_cols)

  # 3. Calcular el mapeo de filas (Excel empieza en 2 por el header)
  row_lookup_dt <- export_dt[, .(excel_row = .I + 1L), by = .(source_row_index)]

  # 3. CREAR WORKBOOK Y ESCRIBIR DATOS
  fs::dir_create(fs::path_dir(output_path))
  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "audit_report")

  # Escribimos solo las columnas de datos (sin las técnicas)
  openxlsx::writeData(workbook, "audit_report", export_dt[, ..cols_to_show])

  # 4. LÓGICA DE RESALTADO (Highlighting)
  effective_findings_dt <- findings_dt
  if (
    is.null(effective_findings_dt) &&
      all(c("row_index", "audit_column") %in% names(export_dt))
  ) {
    effective_findings_dt <- unique(export_dt[, .(row_index, audit_column)])
  }

  if (
    !is.null(effective_findings_dt) &&
      nrow(export_dt) > 0 &&
      nrow(effective_findings_dt) > 0
  ) {
    style_config <- config$export_config$styles$error_highlight
    highlight_style <- openxlsx::createStyle(
      fgFill = style_config$fgFill,
      fontColour = style_config$fontColour,
      textDecoration = style_config$textDecoration
    )

    # Unir hallazgos con el número de fila real en Excel
    findings_to_style <- data.table::as.data.table(effective_findings_dt)
    findings_to_style <- findings_to_style[
      !is.na(row_index) & nzchar(audit_column)
    ]

    if (nrow(findings_to_style) > 0) {
      findings_to_style[, source_row_index := as.integer(row_index)]

      # Merge con el lookup que calculamos al principio
      findings_to_style <- merge(
        findings_to_style,
        row_lookup_dt,
        by = "source_row_index",
        all.x = FALSE
      )

      # Mapear nombres de columnas a su posición actual en el Excel (1, 2, 3...)
      column_index_map <- setNames(seq_along(cols_to_show), cols_to_show)

      # Filtrar hallazgos que correspondan a columnas que realmente estamos mostrando
      findings_to_style <- findings_to_style[audit_column %in% cols_to_show]

      if (nrow(findings_to_style) > 0) {
        findings_to_style[, excel_col := unname(column_index_map[audit_column])]

        # Aplicar estilos agrupados por columna para mejorar performance
        style_groups <- split(
          findings_to_style$excel_row,
          findings_to_style$excel_col
        )

        purrr::walk(names(style_groups), function(col_idx_chr) {
          col_idx <- as.integer(col_idx_chr)
          rows_to_paint <- style_groups[[col_idx_chr]]

          openxlsx::addStyle(
            workbook,
            sheet = "audit_report",
            style = highlight_style,
            rows = rows_to_paint,
            cols = rep(col_idx, length(rows_to_paint)),
            gridExpand = FALSE
          )
        })
      }
    }
  }

  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)
  return(output_path)
}

#' @title create audited data output
#' @description identify invalid rows, export audit artifacts when findings are
#' present, mirror related raw files, and return a typed output where `value` is
#' parsed as numeric.
#' @param dataset_dt data frame or data table to audit.
#' @param config named audit configuration list.
#' @return data table with the same rows as `dataset_dt` and numeric-parsed
#' `value` column.
#' @examples
#' # audit_data_output(dataset_dt, config)
audit_data_output <- function(dataset_dt, config) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(
    config,
    any.missing = FALSE,
    min.len = 1
  ))
  load_audit_config(config)
  assert_or_abort(checkmate::check_directory_exists(
    config$paths$data$imports$raw
  ))

  audit_result <- identify_audit_errors(
    dataset_dt,
    config,
    include_findings = TRUE
  )
  audit_dt <- audit_result$audit_dt

  if (nrow(audit_dt) > 0) {
    export_validation_audit_report(
      audit_dt = audit_dt,
      config = config,
      findings_dt = audit_result$findings_dt,
      output_path = config$paths$data$audit$audit_file_path
    )
    mirror_raw_import_errors(
      audit_dt = audit_dt,
      raw_imports_dir = config$paths$data$imports$raw,
      raw_imports_mirror_dir = config$paths$data$audit$raw_imports_mirror_dir
    )
  }

  audited_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  audited_dt[,
    value := suppressWarnings(readr::parse_double(
      as.character(value),
      na = c("", "na", "nan", "null")
    ))
  ]
  audited_dt
}
