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
    return(data.table::data.table(
      row_index = integer(),
      audit_column = character(),
      audit_type = character(),
      audit_message = character()
    ))
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
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(column_name, min.chars = 1)
  checkmate::assert_names(names(dataset_dt), must.include = column_name)
  checkmate::assert_atomic(dataset_dt[[column_name]], any.missing = TRUE)

  column_values <- as.character(dataset_dt[[column_name]])
  invalid_rows <- which(
    !is.na(column_values) & !grepl("^[0-9]+(\\.[0-9]+)?$", column_values)
  )

  if (length(invalid_rows) == 0) {
    return(data.table::data.table(
      row_index = integer(),
      audit_column = character(),
      audit_type = character(),
      audit_message = character()
    ))
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
  }) |> purrr::flatten()

  findings_dt <- data.table::rbindlist(findings_list, fill = TRUE)
  if (nrow(findings_dt) == 0) {
    findings_dt <- data.table::data.table(
      row_index = integer(),
      audit_column = character(),
      audit_type = character(),
      audit_message = character()
    )
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
#' @return data table containing only invalid rows.
#' @examples
#' # identify_audit_errors(fao_data_raw, config)
identify_audit_errors <- function(fao_data_raw, config) {
  assert_or_abort(checkmate::check_data_frame(fao_data_raw, min.rows = 0))
  assert_or_abort(checkmate::check_list(
    config,
    any.missing = FALSE,
    min.len = 1
  ))
  load_audit_config(config)
  assert_or_abort(checkmate::check_names(
    names(fao_data_raw),
    must.include = config$column_order
  ))

  audit_result <- run_audit_by_type(dataset_dt = fao_data_raw, config = config)

  audit_dt <- data.table::as.data.table(fao_data_raw)
  output_dt <- data.table::copy(audit_dt[audit_result$invalid_row_index])
  if (nrow(output_dt) > 0) {
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
#' @param output_path character scalar destination path for the excel file.
#' @return character scalar with written output path.
#' @examples
#' # export_validation_audit_report(audit_dt)
export_validation_audit_report <- function(
  audit_dt,
  output_path = fs::path(here::here("data", "audit"), "audit.xlsx")
) {
  assert_or_abort(checkmate::check_data_frame(audit_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(output_path, min.chars = 1))
  assert_or_abort(checkmate::check_names(
    names(audit_dt),
    must.include = "document"
  ))

  export_dt <- data.table::copy(data.table::as.data.table(audit_dt))
  data.table::setorderv(export_dt, cols = "document", na.last = TRUE)

  fs::dir_create(fs::path_dir(output_path))
  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "audit_report")
  openxlsx::writeData(workbook, "audit_report", export_dt)
  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)

  output_path
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

  audit_dt <- identify_audit_errors(dataset_dt, config)
  if (nrow(audit_dt) > 0) {
    export_validation_audit_report(
      audit_dt,
      config$paths$data$audit$audit_file_path
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
