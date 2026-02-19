# script: data audit script
# description: validate consolidated import data, isolate invalid records,
# and export audit artifacts with mirrored raw source files.

#' @title prepare audit root directory
#' @description safely remove previous audit folder if it exists.
#' only creates the audit folder when files are to be written.
#' @param audit_root_dir character scalar path to the root audit folder.
#' @return invisible logical scalar: TRUE if folder existed and was deleted, FALSE otherwise.
#' @examples
#' # prepare_audit_root("data/audit")
#' @export
prepare_audit_root <- function(audit_root_dir) {
  assert_or_abort(checkmate::check_string(audit_root_dir, min.chars = 1))

  if (fs::dir_exists(audit_root_dir)) {
    tryCatch(
      {
        fs::dir_delete(audit_root_dir)
        invisible(TRUE)
      },
      error = function(e) {
        cli::cli_abort(
          "failed to delete existing audit folder {.path {audit_root_dir}}: {e$message}"
        )
      }
    )
  } else {
    invisible(FALSE)
  }
}

#' @title empty audit findings data table
#' @description create a standardized empty audit findings data table.
#' @return data.table with predefined audit columns.
#' @examples
#' empty_audit_findings_dt()
#' @export
# script: data audit script
# description: validate consolidated import data, isolate invalid records,
# and export audit artifacts with mirrored raw source files.
empty_audit_findings_dt <- function() {
  data.table::data.table(
    row_index = integer(),
    audit_column = character(),
    audit_type = character(),
    audit_message = character()
  )
}

#' @title load audit configuration
#' @description validate required audit configuration fields used by the import audit pipeline.
#' @param config named list containing required configuration elements.
#' @return invisible TRUE when validation succeeds.
#' @examples
#' # load_audit_config(config)
#' @export
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
    purrr::walk(config$audit_columns_by_type, function(x) {
      assert_or_abort(checkmate::check_character(
        x,
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

#' @title resolve audit output paths
#' @description compute audit output paths without creating directories.
#' @param audit_root_dir character scalar root audit directory.
#' @param audit_file_name character scalar excel file name.
#' @param mirror_dir_name character scalar mirror directory name.
#' @return named list with audit_file_path and mirror_dir_path.
#' @examples
#' resolve_audit_output_paths("data/audit", "audit.xlsx", "mirror")
#' @export
resolve_audit_output_paths <- function(
  audit_root_dir,
  audit_file_name,
  mirror_dir_name
) {
  assert_or_abort(checkmate::check_string(audit_root_dir, min.chars = 1))
  assert_or_abort(checkmate::check_string(audit_file_name, min.chars = 1))
  assert_or_abort(checkmate::check_string(mirror_dir_name, min.chars = 1))

  list(
    audit_root_dir = audit_root_dir,
    audit_file_path = fs::path(audit_root_dir, audit_file_name),
    mirror_dir_path = fs::path(audit_root_dir, mirror_dir_name)
  )
}

#' @title audit non-empty character values
#' @description validate that values are non-missing and non-empty.
#' @param dataset_dt data frame.
#' @param column_name character scalar.
#' @return data.table of findings.
#' @examples
#' # audit_character_non_empty(df, "document")
#' @export
audit_character_non_empty <- function(dataset_dt, column_name) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(
    names(dataset_dt),
    must.include = column_name
  ))

  values <- dataset_dt[[column_name]]
  invalid_rows <- which(is.na(values) | !nzchar(trimws(values)))

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
#' @description validate numeric string pattern.
#' @param dataset_dt data frame.
#' @param column_name character scalar.
#' @return data.table of findings.
#' @examples
#' # audit_numeric_string(df)
#' @export
audit_numeric_string <- function(dataset_dt, column_name = "value") {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_string(column_name, min.chars = 1))
  assert_or_abort(checkmate::check_names(
    names(dataset_dt),
    must.include = column_name
  ))

  values <- as.character(dataset_dt[[column_name]])
  invalid_rows <- which(!is.na(values) & !grepl("^[0-9]+(\\.[0-9]+)?$", values))

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
#' @description execute configured validators.
#' @param dataset_dt data frame.
#' @param audit_columns_by_type named list.
#' @param selected_validations optional character vector.
#' @return named list with findings and invalid_row_index.
#' @examples
#' # run_master_validation(df, audit_map)
#' @export
run_master_validation <- function(
  dataset_dt,
  audit_columns_by_type,
  selected_validations = NULL
) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(audit_columns_by_type, min.len = 1))

  registry <- list(
    character_non_empty = audit_character_non_empty,
    numeric_string = audit_numeric_string
  )

  audit_types <- names(audit_columns_by_type)
  supported <- intersect(audit_types, names(registry))

  findings <- purrr::map(supported, function(type) {
    purrr::map(
      audit_columns_by_type[[type]],
      function(col) registry[[type]](dataset_dt, col)
    )
  }) |>
    purrr::flatten()

  findings_dt <- data.table::rbindlist(findings, fill = TRUE)
  if (nrow(findings_dt) == 0) {
    findings_dt <- empty_audit_findings_dt()
  }

  list(
    findings = findings_dt,
    invalid_row_index = sort(unique(findings_dt$row_index))
  )
}

#' @title resolve audit columns by validation type
#' @description return audit columns grouped by validator type. if
#' config$audit_columns_by_type is present, it is returned. otherwise,
#' a default mapping is created from config$audit_columns and config$column_order.
#' @param config named list with audit configuration.
#' @return named list mapping audit types to column names.
#' @examples
#' # resolve_audit_columns_by_type(config)
#' @export
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

#' @title export validation audit report
#' @description write audit results to an excel workbook at output_path.
#' Only creates folders and workbook if there is data to export.
#' Output is sorted by document and written to sheet audit_report
#' with specific cells highlighted based on config styles.
#' @param audit_dt data frame or data table containing at least document.
#' @param config named audit configuration list containing export styles.
#' @param findings_dt data table with row_index and audit_column.
#' @param output_path character scalar destination path for the excel file.
#' @return character scalar with written output path (or NULL if nothing written).
#' @examples
#' # export_validation_audit_report(audit_dt, config)
#' @export
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
  assert_or_abort(checkmate::check_string(output_path, min.chars = 1))

  load_audit_config(config)

  # nothing to export
  if (nrow(audit_dt) == 0) {
    return(invisible(NULL))
  }

  export_dt <- data.table::as.data.table(data.table::copy(audit_dt))

  # create source row index
  export_dt[,
    source_row_index := if ("row_index" %in% names(export_dt)) {
      row_index
    } else {
      seq_len(.N)
    }
  ]

  # sort by document
  if ("document" %in% names(export_dt)) {
    data.table::setorderv(export_dt, cols = "document", na.last = TRUE)
  }

  technical_cols <- c(
    "source_row_index",
    "row_index",
    "audit_column",
    "audit_type",
    "audit_message"
  )
  cols_to_show <- setdiff(names(export_dt), technical_cols)
  row_lookup_dt <- export_dt[, .(excel_row = .I + 1L), by = .(source_row_index)]

  # create workbook only if there is data
  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "audit_report")
  openxlsx::writeData(workbook, "audit_report", export_dt[, ..cols_to_show])

  # determine effective findings
  effective_findings_dt <- findings_dt
  if (
    is.null(effective_findings_dt) &&
      all(c("row_index", "audit_column") %in% names(export_dt))
  ) {
    effective_findings_dt <- unique(export_dt[, .(row_index, audit_column)])
  }

  # highlight errors if any
  if (
    !is.null(effective_findings_dt) &&
      nrow(export_dt) > 0 &&
      nrow(effective_findings_dt) > 0
  ) {
    style_config <- config$export_config$styles$error_highlight
    highlight_style <- do.call(openxlsx::createStyle, style_config)

    findings_to_style <- data.table::as.data.table(effective_findings_dt)
    findings_to_style <- findings_to_style[
      !is.na(row_index) & nzchar(audit_column)
    ]

    if (nrow(findings_to_style) > 0) {
      findings_to_style[, source_row_index := as.integer(row_index)]
      findings_to_style <- merge(
        findings_to_style,
        row_lookup_dt,
        by = "source_row_index",
        all.x = FALSE
      )
      column_index_map <- setNames(seq_along(cols_to_show), cols_to_show)
      findings_to_style <- findings_to_style[audit_column %in% cols_to_show]

      if (nrow(findings_to_style) > 0) {
        findings_to_style[, excel_col := unname(column_index_map[audit_column])]
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

  ensure_output_directories(output_path)

  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)
  output_path
}


#' @title mirror raw import errors
#' @description copy raw import files associated with invalid audit records
#' into a mirror directory while preserving relative folder structure.
#' @param audit_dt data frame or data table containing at least document.
#' @param raw_imports_dir character scalar path to raw import files.
#' @param raw_imports_mirror_dir character scalar path to mirror destination.
#' @return invisible character vector of mirrored target file paths.
#' @examples
#' # mirror_raw_import_errors(audit_dt, "data/imports/raw", "data/audit/mirror")
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

  ensure_output_directories(target_paths)
  fs::file_copy(matched_paths, target_paths, overwrite = TRUE)

  invisible(as.character(target_paths))
}

#' @title create audited data output
#' @description run audit, export excel, mirror files, and return numeric-parsed data.
#' @param dataset_dt data frame.
#' @param config audit configuration.
#' @return data.table.
#' @export
audit_data_output <- function(dataset_dt, config) {
  assert_or_abort(checkmate::check_data_frame(dataset_dt, min.rows = 0))
  assert_or_abort(checkmate::check_list(config, min.len = 1))

  load_audit_config(config)

  audit_root_dir <- config$paths$data$audit$audit_root_dir
  audit_output_dir <- fs::path_dir(config$paths$data$audit$audit_file_path)

  assert_or_abort(checkmate::check_string(audit_root_dir, min.chars = 1))
  assert_or_abort(checkmate::check_string(audit_output_dir, min.chars = 1))

  prepare_audit_root(audit_root_dir)

  audit_result <- run_master_validation(
    dataset_dt,
    resolve_audit_columns_by_type(config)
  )

  invalid_index <- audit_result$invalid_row_index

  if (length(invalid_index) > 0) {
    # subset invalid rows
    audit_dt <- dataset_dt[invalid_index, , drop = FALSE]

    # remap findings row_index to local subset positions
    findings_dt <- data.table::as.data.table(audit_result$findings)

    if (nrow(findings_dt) > 0) {
      index_map <- data.table::data.table(
        original_row_index = invalid_index,
        local_row_index = seq_along(invalid_index)
      )

      findings_dt <- merge(
        findings_dt,
        index_map,
        by.x = "row_index",
        by.y = "original_row_index",
        all.x = TRUE
      )

      findings_dt[, row_index := local_row_index]
      findings_dt[, local_row_index := NULL]
    }

    prepared_paths <- resolve_audit_output_paths(
      audit_root_dir = audit_output_dir,
      audit_file_name = fs::path_file(config$paths$data$audit$audit_file_path),
      mirror_dir_name = fs::path_file(
        config$paths$data$audit$raw_imports_mirror_dir
      )
    )

    export_validation_audit_report(
      audit_dt = audit_dt,
      config = config,
      findings_dt = findings_dt,
      output_path = prepared_paths$audit_file_path
    )

    mirror_raw_import_errors(
      audit_dt = audit_dt,
      raw_imports_dir = config$paths$data$imports$raw,
      raw_imports_mirror_dir = prepared_paths$mirror_dir_path
    )
  }

  audited_dt <- data.table::as.data.table(dataset_dt)

  if ("value" %in% names(audited_dt)) {
    audited_dt[,
      value := suppressWarnings(readr::parse_double(as.character(value)))
    ]
  }

  audited_dt
}
