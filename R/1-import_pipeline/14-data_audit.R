# script: data audit script
# description: audit consolidated import data, export validation findings,
# and mirror raw source files that produced invalid records.

#' @title validate required audit config fields
#' @description validate that the audit pipeline receives all required config
#' fields used by data audit and mirroring operations.
#' @param config named list containing audit and import paths plus audit column
#' metadata.
#' @return invisible true when config validation succeeds.
#' @importFrom checkmate check_list check_character check_string
#' @examples
#' config_example <- list(
#'   column_order = c("continent", "country", "product", "document", "value"),
#'   audit_columns = c("continent", "country", "product"),
#'   audit_columns_by_type = list(
#'     character_non_empty = c("continent", "country", "product"),
#'     numeric_string = "value"
#'   ),
#'   paths = list(
#'     data = list(
#'       imports = list(raw = tempdir()),
#'       audit = list(
#'         audit_file_path = file.path(tempdir(), "audit.xlsx"),
#'         raw_imports_mirror_dir = file.path(tempdir(), "mirror")
#'       )
#'     )
#'   )
#' )
#' validate_audit_config(config_example)
#' @export
validate_audit_config <- function(config) {
  assert_or_abort(checkmate::check_list(config, min.len = 1, any.missing = FALSE))

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

    for (audit_columns in config$audit_columns_by_type) {
      assert_or_abort(checkmate::check_character(
        audit_columns,
        min.len = 1,
        any.missing = FALSE
      ))
    }
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

#' @title resolve audit columns by audit type
#' @description build a type-driven audit column map from config. when
#' `config$audit_columns_by_type` is absent, the function applies a backward
#' compatible fallback that maps `config$audit_columns` to
#' `"character_non_empty"` and maps `"value"` to `"numeric_string"`.
#' @param config named list containing `column_order`, `audit_columns`, and
#' optional `audit_columns_by_type`.
#' @return named list with character vectors for audit types.
#' @importFrom checkmate check_list
#' @examples
#' config_example <- list(
#'   column_order = c("continent", "document", "value"),
#'   audit_columns = c("continent", "document")
#' )
#' resolve_audit_columns_by_type(config_example)
#' @export
resolve_audit_columns_by_type <- function(config) {
  assert_or_abort(checkmate::check_list(config, min.len = 1, any.missing = FALSE))
  validate_audit_config(config)

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

#' @title audit character column for non-empty values
#' @description identify rows where a configured character column is missing,
#' empty, or whitespace-only.
#' @param dataset_dt data frame or data table containing consolidated raw
#' observations.
#' @param column_name character scalar with the column to audit.
#' @return data table with structured row-level findings. columns are
#' `row_index`, `audit_column`, `audit_type`, and `audit_message`.
#' @importFrom checkmate check_data_frame check_string check_names check_character
#' @importFrom data.table data.table
#' @examples
#' data_example <- data.frame(continent = c("asia", " ", NA_character_))
#' audit_character_non_empty_column(data_example, "continent")
#' @export
audit_character_non_empty_column <- function(dataset_dt, column_name) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(names(dataset_dt), must.include = column_name))
  assert_or_abort(checkmate::check_character(
    dataset_dt[[column_name]],
    any.missing = TRUE,
    null.ok = FALSE
  ))

  column_values <- dataset_dt[[column_name]]
  is_valid_value <- !is.na(column_values) & nzchar(trimws(column_values))
  invalid_rows <- which(!is_valid_value)

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

#' @title audit numeric-string column
#' @description identify rows where a configured numeric-string column has
#' invalid values. valid values contain only digits (`0`-`9`) and at most one
#' decimal point.
#' @param dataset_dt data frame or data table containing consolidated raw
#' observations.
#' @param column_name character scalar with the column to audit.
#' @return data table with structured row-level findings. columns are
#' `row_index`, `audit_column`, `audit_type`, and `audit_message`.
#' @importFrom checkmate check_data_frame check_string check_names check_atomic
#' @importFrom data.table data.table
#' @examples
#' data_example <- data.frame(value = c("10", "10.5", "10a"))
#' audit_numeric_string_column(data_example, "value")
#' @export
audit_numeric_string_column <- function(dataset_dt, column_name = "value") {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(names(dataset_dt), must.include = column_name))
  assert_or_abort(checkmate::check_atomic(
    dataset_dt[[column_name]],
    any.missing = TRUE,
    null.ok = FALSE
  ))

  column_values <- as.character(dataset_dt[[column_name]])
  is_valid_value <- !is.na(column_values) & grepl(
    pattern = "^[0-9]+(\\\\.[0-9]+)?$",
    x = column_values
  )
  invalid_rows <- which(!is_valid_value)

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

#' @title run configured data audits by column type
#' @description execute audit functions by configured type and return a
#' consolidated structured object with row-level findings and unique invalid row
#' indices.
#' @param dataset_dt data frame or data table containing consolidated raw
#' observations.
#' @param config named list containing audit settings.
#' @return list with two elements: `findings` (data table) and
#' `invalid_row_index` (integer vector).
#' @importFrom checkmate check_data_frame check_list check_names check_string
#' @importFrom data.table data.table rbindlist
#' @examples
#' config_example <- list(
#'   column_order = c("continent", "document", "value"),
#'   audit_columns = c("continent", "document")
#' )
#' data_example <- data.frame(
#'   continent = c("asia", ""),
#'   document = c("ok.xlsx", "bad.xlsx"),
#'   value = c("10", "10a")
#' )
#' run_audit_by_type(data_example, config_example)
#' @export
run_audit_by_type <- function(dataset_dt, config) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE, min.len = 1))
  validate_audit_config(config)

  audit_columns_by_type <- resolve_audit_columns_by_type(config)

  audit_function_registry <- list(
    character_non_empty = audit_character_non_empty_column,
    numeric_string = audit_numeric_string_column
  )

  findings_list <- list()

  for (audit_type in names(audit_columns_by_type)) {
    assert_or_abort(checkmate::check_string(audit_type, min.chars = 1))

    if (is.null(audit_function_registry[[audit_type]])) {
      cli::cli_warn(
        "unsupported audit type {.val {audit_type}} was skipped during data audit"
      )
      next
    }

    for (column_name in audit_columns_by_type[[audit_type]]) {
      assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
      assert_or_abort(checkmate::check_names(
        names(dataset_dt),
        must.include = column_name
      ))

      findings_list[[length(findings_list) + 1]] <- audit_function_registry[[audit_type]](
        dataset_dt = dataset_dt,
        column_name = column_name
      )
    }
  }

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

#' @title identify row-level audit errors for consolidated data
#' @description subset consolidated `fao_data_raw` rows that have failed checks
#' in configured audit column types.
#' @param fao_data_raw data frame or data table containing consolidated raw fao
#' observations.
#' @param config named list containing `column_order`, `audit_columns`, and
#' optional `audit_columns_by_type`.
#' @return `data.table` containing only rows with at least one audit finding,
#' sorted alphabetically by `document`.
#' @importFrom checkmate check_data_frame check_names check_list
#' @importFrom data.table as.data.table copy setorderv
#' @examples
#' config_example <- list(
#'   column_order = c("continent", "country", "product", "document", "value"),
#'   audit_columns = c("continent", "country", "product")
#' )
#' data_example <- data.frame(
#'   continent = c("asia", ""),
#'   country = c("nepal", "nepal"),
#'   product = c("rice", "rice"),
#'   document = c("clean_file.xlsx", "dirty_file.xlsx"),
#'   value = c("1.2", "1a")
#' )
#' identify_audit_errors(data_example, config_example)
#' @export
identify_audit_errors <- function(fao_data_raw, config) {
  assert_or_abort(checkmate::check_data_frame(fao_data_raw, min.rows = 0))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE, min.len = 1))
  validate_audit_config(config)

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

#' @title mirror raw import errors into dataset audit folder
#' @description copy only raw import files that produced validation errors into
#' a dataset-specific mirror directory while preserving relative paths below the
#' configured raw imports root.
#' @param audit_dt data table containing dirty rows and a `document` column.
#' @param raw_imports_dir character scalar path to source raw imports.
#' @param raw_imports_mirror_dir character scalar target directory used to store
#' mirrored error-source files.
#' @return invisible character vector of mirrored file paths.
#' @importFrom checkmate check_data_frame check_string check_names check_directory_exists
#' @importFrom fs dir_create dir_ls path_file path_rel path_dir file_copy path
#' @importFrom cli cli_warn
#' @examples
#' # mirror_raw_import_errors(audit_dt, "data/imports/raw imports", "data/audit/fao_data_raw/raw_imports_mirror")
#' @export
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
  assert_or_abort(checkmate::check_string(raw_imports_mirror_dir, min.chars = 1))
  assert_or_abort(checkmate::check_directory_exists(raw_imports_dir))

  error_documents <- unique(as.character(audit_dt$document))
  error_documents <- error_documents[!is.na(error_documents) & nzchar(error_documents)]

  if (length(error_documents) == 0) {
    return(invisible(character(0)))
  }

  raw_files <- fs::dir_ls(
    path = raw_imports_dir,
    type = "file",
    recurse = TRUE,
    glob = "*.xlsx"
  )

  if (length(raw_files) == 0) {
    cli::cli_warn(
      "no raw import files were found under {.path {raw_imports_dir}} for mirroring"
    )
    return(invisible(character(0)))
  }

  raw_file_names <- fs::path_file(raw_files)
  selected_rows <- raw_file_names %in% error_documents

  if (!any(selected_rows)) {
    cli::cli_warn("no matching raw import files were found for mirrored audit output")
    return(invisible(character(0)))
  }

  unmatched_documents <- setdiff(error_documents, raw_file_names)
  if (length(unmatched_documents) > 0) {
    cli::cli_warn(
      c(
        "some audited documents were not found in raw imports",
        "i" = "missing files: {toString(unmatched_documents)}"
      )
    )
  }

  matched_paths <- raw_files[selected_rows]
  relative_paths <- fs::path_rel(matched_paths, start = raw_imports_dir)
  target_paths <- fs::path(raw_imports_mirror_dir, relative_paths)

  fs::dir_create(fs::path_dir(target_paths))
  fs::file_copy(matched_paths, target_paths, overwrite = TRUE)

  invisible(as.character(target_paths))
}

#' @title export validation audit report to excel
#' @description write row-level validation errors to an excel workbook for
#' manual review and return the resolved output path. before export, rows are
#' sorted alphabetically by `document`.
#' @param audit_dt data table containing dirty rows.
#' @param output_path character scalar output path for the excel file. the
#' default is a generic project audit file and should usually be overridden by
#' `config$paths$data$audit$audit_file_path`.
#' @return character scalar with the saved excel path.
#' @importFrom checkmate check_data_frame check_string check_names
#' @importFrom fs dir_create path_dir path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' # export_validation_audit_report(data.table::data.table(document = "sample.xlsx"))
#' @export
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

#' @title validate consolidated data for analytical readiness
#' @description validate consolidated dataset rows, export an excel audit report
#' when dirty rows are detected, mirror raw error imports into the
#' dataset-specific audit folder, and continue execution with a validated
#' `data.table` where `value` is numeric.
#' @param dataset_dt data frame or data table containing consolidated raw
#' observations.
#' @param config named list containing dataset-specific audit and raw import
#' paths under `paths$data`.
#' @return validated `data.table` with `value` coerced to numeric.
#' @importFrom checkmate check_data_frame check_list check_directory_exists
#' @importFrom data.table as.data.table copy
#' @importFrom readr parse_double
#' @importFrom cli cli_alert_info
#' @examples
#' # audit_data_output(data_example, load_pipeline_config("fao_data_raw"))
#' @export
audit_data_output <- function(dataset_dt, config) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE, min.len = 1))
  validate_audit_config(config)
  assert_or_abort(checkmate::check_directory_exists(config$paths$data$imports$raw))

  audit_dt <- identify_audit_errors(dataset_dt, config)

  if (nrow(audit_dt) > 0) {
    report_path <- export_validation_audit_report(
      audit_dt = audit_dt,
      output_path = config$paths$data$audit$audit_file_path
    )

    mirror_raw_import_errors(
      audit_dt = audit_dt,
      raw_imports_dir = config$paths$data$imports$raw,
      raw_imports_mirror_dir = config$paths$data$audit$raw_imports_mirror_dir
    )

    cli::cli_alert_info("audit report written to {.path {report_path}}")
  }

  audited_dt <- data.table::copy(data.table::as.data.table(dataset_dt))

  audited_dt[
    ,
    value := suppressWarnings(
      readr::parse_double(as.character(value), na = c("", "na", "nan", "null"))
    )
  ]

  audited_dt
}
