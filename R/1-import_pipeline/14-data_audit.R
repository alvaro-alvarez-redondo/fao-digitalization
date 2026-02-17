# script: data audit script
# description: audit consolidated import data, export validation findings,
# and mirror raw source files that produced invalid records.

# -------------------------------
# config validation
# -------------------------------

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

# -------------------------------
# individual validators
# -------------------------------

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

# -------------------------------
# master validator
# -------------------------------

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
    audit_columns_by_type <- audit_columns_by_type[
      names(audit_columns_by_type) %in% selected_validations
    ]
  }

  findings_list <- list()
  for (audit_type in names(audit_columns_by_type)) {
    if (is.null(validator_registry[[audit_type]])) {
      cli::cli_warn("unsupported audit type {.val {audit_type}} was skipped")
      next
    }
    for (column_name in audit_columns_by_type[[audit_type]]) {
      findings_list[[length(findings_list) + 1]] <- validator_registry[[
        audit_type
      ]](
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

# backward compatible wrapper
run_audit_by_type <- function(dataset_dt, config) {
  audit_columns_by_type <- resolve_audit_columns_by_type(config)
  run_master_validation(dataset_dt, audit_columns_by_type)
}

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

  # Clear the mirror directory before copying
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
