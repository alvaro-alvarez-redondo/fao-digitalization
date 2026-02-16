# script: validate log scripts
# description: validate mandatory fields, detect duplicates and collect errors/warnings

#' @title validate mandatory fields data table
#' @description validate mandatory columns in a long-format table, create missing
#' mandatory columns as `na_character_`, ensure a `document` column exists, and
#' generate unique row-level error messages for missing mandatory values.
#' @param dt data table or data frame in long format.
#' @param config named list containing `column_required` as a non-empty character
#' vector.
#' @return named list with `errors` as a character vector and `data` as a data
#' table with normalized mandatory columns.
#' @importFrom checkmate assert_data_frame assert_string assert_list assert_character
#' @importFrom data.table as.data.table
#' @importFrom tidyr pivot_longer
#' @importFrom tidyselect all_of
#' @importFrom dplyr filter mutate
#' @examples
#' dt_example <- data.frame(product = "a", variable = "b", year = "2020", value = "", document = "doc.xlsx")
#' config_example <- list(column_required = c("product", "variable", "year", "value"))
#' validate_mandatory_fields_dt(dt_example, config_example)
validate_mandatory_fields_dt <- function(dt, config) {
  checkmate::assert_data_frame(dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  )

  dt <- data.table::as.data.table(dt)
  mandatory_cols <- config$column_required

  missing_mandatory_cols <- setdiff(mandatory_cols, colnames(dt))

  if (length(missing_mandatory_cols) > 0) {
    dt[, (missing_mandatory_cols) := NA_character_]
  }

  if (!("document" %in% colnames(dt))) {
    dt[, document := "unknown_document"]
  }

  missing_long <- dt[, row_id := .I][] |>
    tidyr::pivot_longer(
      cols = tidyselect::all_of(mandatory_cols),
      names_to = "column_name",
      values_to = "column_value"
    ) |>
    dplyr::filter(is.na(column_value) | column_value == "") |>
    dplyr::mutate(
      error_message = paste0(
        "missing mandatory value in document '",
        document,
        "', row_id '",
        row_id,
        "', column '",
        column_name,
        "'"
      )
    )

  errors <- unique(missing_long$error_message)
  dt[, row_id := NULL]

  list(errors = errors, data = dt)
}

#' @title detect duplicates data table
#' @description detect duplicate rows using the long-grain key
#' (`product`, `variable`, `year`, `value`, `document`) and return duplicate
#' diagnostics as error messages.
#' @param dt data table or data frame containing long-format observations.
#' @return named list with `errors` as a character vector and `data` as the
#' unchanged data table.
#' @importFrom checkmate assert_data_frame assert_string assert_names
#' @importFrom data.table as.data.table
#' @examples
#' dt_example <- data.frame(
#'   product = c("a", "a"),
#'   variable = c("b", "b"),
#'   year = c("2020", "2020"),
#'   value = c("1", "1"),
#'   document = c("doc.xlsx", "doc.xlsx")
#' )
#' detect_duplicates_dt(dt_example)
detect_duplicates_dt <- function(dt) {
  checkmate::assert_data_frame(dt)
  checkmate::assert_names(
    names(dt),
    must.include = c("product", "variable", "year", "value", "document"),
    what = "names(dt)"
  )

  dt <- data.table::as.data.table(dt)

  dup_counts <- dt[,
    .(duplicate_count = .N),
    by = .(product, variable, year, value, document)
  ]

  dup_rows <- dup_counts[duplicate_count > 1]

  errors <- if (nrow(dup_rows) > 0) {
    paste0(
      "duplicate entries detected for product '",
      dup_rows$product,
      "', variable '",
      dup_rows$variable,
      "', year '",
      dup_rows$year,
      "', value '",
      dup_rows$value,
      "', duplicate_count '",
      dup_rows$duplicate_count,
      "' in document '",
      dup_rows$document,
      "'"
    )
  } else {
    character(0)
  }

  list(errors = errors, data = dt)
}

#' @title validate long data table
#' @description run the complete long-table validation pipeline by applying
#' mandatory field checks and duplicate detection, and return a validated table
#' with aggregated error messages.
#' @param long_dt data table or data frame containing long-format records.
#' @param config named list containing `column_required` as a non-empty character
#' vector.
#' @return named list with `data` as a data table and `errors` as a character
#' vector of validation issues.
#' @importFrom checkmate assert_data_frame assert_string assert_list assert_character
#' @importFrom data.table as.data.table
#' @examples
#' long_dt_example <- data.frame(product = "a", variable = "b", year = "2020", value = "1", document = "doc.xlsx")
#' config_example <- list(column_required = c("product", "variable", "year", "value"))
#' validate_long_dt(long_dt_example, config_example)
validate_long_dt <- function(long_dt, config) {
  checkmate::assert_data_frame(long_dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  )

  dt <- data.table::as.data.table(long_dt)

  mandatory_result <- validate_mandatory_fields_dt(dt, config)
  duplicate_result <- detect_duplicates_dt(mandatory_result$data)

  list(
    data = mandatory_result$data,
    errors = c(mandatory_result$errors, duplicate_result$errors)
  )
}


#' @title identify row-level validation errors for consolidated data
#' @description audits consolidated `fao_data_raw` rows against validation rules
#' and returns only dirty rows. the function builds a row-level `error_columns`
#' field listing only columns that fail in each specific row, separated by `", "`.
#' @param fao_data_raw data frame or data table containing consolidated raw fao
#' observations.
#' @return `data.table` containing only rows with at least one validation error,
#' with `error_columns` as the first column.
#' @importFrom checkmate assert_data_frame assert_string assert_names
#' @importFrom data.table as.data.table copy data.table setcolorder
#' @importFrom readr parse_double
#' @importFrom stringr str_detect
#' @examples
#' data_example <- data.frame(
#'   continent = "asia",
#'   country = "nepal",
#'   product = "rice",
#'   variable = "production",
#'   unit = "t",
#'   year = "2020",
#'   value = "1.2",
#'   notes = NA_character_,
#'   footnotes = "none",
#'   yearbook = "yb_2020",
#'   document = "sample_file.xlsx"
#' )
#' identify_validation_errors(data_example)
identify_validation_errors <- function(fao_data_raw) {
  checkmate::assert_data_frame(fao_data_raw)

  required_columns <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "value",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

  mandatory_character_columns <- c(
    "continent",
    "country",
    "unit",
    "product",
    "variable",
    "year",
    "yearbook",
    "document"
  )

  optional_character_columns <- c("footnotes", "notes")

  checkmate::assert_names(
    names(fao_data_raw),
    must.include = required_columns,
    what = "names(fao_data_raw)"
  )

  audit_dt <- fao_data_raw |>
    data.table::as.data.table() |>
    data.table::copy()

  row_count <- nrow(audit_dt)
  error_flags <- vector("list", length(required_columns))
  names(error_flags) <- required_columns

  for (column_name in required_columns) {
    error_flags[[column_name]] <- rep(FALSE, row_count)
  }

  for (column_name in mandatory_character_columns) {
    column_values <- audit_dt[[column_name]]

    if (!is.character(column_values)) {
      error_flags[[column_name]] <- rep(TRUE, row_count)
      next
    }

    error_flags[[column_name]] <-
      is.na(column_values) |
      trimws(column_values) == "" |
      column_values != trimws(column_values)
  }

  for (column_name in optional_character_columns) {
    column_values <- audit_dt[[column_name]]

    if (!is.character(column_values)) {
      error_flags[[column_name]] <- rep(TRUE, row_count)
      next
    }

    error_flags[[column_name]] <-
      (!is.na(column_values) & column_values != trimws(column_values))
  }

  value_raw <- as.character(audit_dt$value)
  value_numeric <- suppressWarnings(
    readr::parse_double(value_raw, na = c("", "na", "nan", "null"))
  )

  value_not_numeric <-
    !is.na(value_raw) &
    nzchar(trimws(value_raw)) &
    is.na(value_numeric)

  value_negative <- !is.na(value_numeric) & value_numeric < 0

  error_flags$value <- value_not_numeric | value_negative

  year_values <- audit_dt$year

  if (!is.character(year_values)) {
    error_flags$year <- error_flags$year | rep(TRUE, row_count)
  } else {
    invalid_year <- !stringr::str_detect(year_values, "^[0-9]{4}(-[0-9]{4})?$")
    error_flags$year <- error_flags$year | invalid_year
  }

  key_columns <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "yearbook",
    "document"
  )

  key_complete <- audit_dt[, rowSums(is.na(.SD)) == 0, .SDcols = key_columns]

  duplicate_idx <- audit_dt[
    key_complete,
    .I[duplicated(.SD) | duplicated(.SD, fromLast = TRUE)],
    .SDcols = key_columns
  ]

  if (length(duplicate_idx) > 0) {
    for (column_name in key_columns) {
      error_flags[[column_name]][duplicate_idx] <- TRUE
    }
  }

  flags_dt <- data.table::as.data.table(error_flags)
  flagged_pairs <- which(as.matrix(flags_dt), arr.ind = TRUE)

  error_columns <- rep("", row_count)

  if (nrow(flagged_pairs) > 0) {
    flagged_dt <- data.table::data.table(
      row_id = flagged_pairs[, "row"],
      column_name = colnames(flags_dt)[flagged_pairs[, "col"]]
    )

    error_by_row <- unique(flagged_dt, by = c("row_id", "column_name"))[,
      .(
        error_columns = paste(column_name, collapse = ", ")
      ),
      by = row_id
    ]

    error_columns[error_by_row$row_id] <- error_by_row$error_columns
  }

  output_dt <- data.table::copy(audit_dt)
  output_dt[, error_columns := as.character(error_columns)]
  data.table::setcolorder(
    output_dt,
    c("error_columns", setdiff(colnames(output_dt), "error_columns"))
  )

  output_dt[nzchar(error_columns)]
}

#' @title mirror raw import errors into dataset audit folder
#' @description copies only raw import files that produced validation errors
#' into a dataset-specific mirror directory while preserving their relative path
#' structure below the configured raw imports root.
#' @param audit_dt data table containing dirty rows and a `document` column.
#' @param raw_imports_dir character scalar path to source raw imports.
#' @param raw_imports_mirror_dir character scalar target directory used to store
#' mirrored error-source files.
#' @return invisible character vector of mirrored file paths.
#' @importFrom checkmate assert_data_frame assert_string assert_names assert_directory_exists
#' @importFrom fs dir_create dir_ls path_file path_rel path_dir file_copy
#' @importFrom purrr map
#' @importFrom cli cli_inform cli_warn
#' @examples
#' # mirror_raw_import_errors(audit_dt, "data/imports/raw imports", "data/audit/fao_data_raw/raw_imports_mirror")
mirror_raw_import_errors <- function(
  audit_dt,
  raw_imports_dir,
  raw_imports_mirror_dir
) {
  checkmate::assert_data_frame(audit_dt)
  checkmate::assert_names(
    names(audit_dt),
    must.include = "document",
    what = "names(audit_dt)"
  )
  checkmate::assert_string(raw_imports_dir, min.chars = 1)
  checkmate::assert_string(raw_imports_mirror_dir, min.chars = 1)
  checkmate::assert_directory_exists(raw_imports_dir)

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
    cli::cli_warn(
      "no raw import files were found under {.path {raw_imports_dir}} for mirroring"
    )
    return(invisible(character(0)))
  }

  raw_file_names <- fs::path_file(raw_files)
  mirrored_targets <- purrr::map(error_documents, \(document_name) {
    matched_paths <- raw_files[raw_file_names == document_name]

    if (length(matched_paths) == 0) {
      cli::cli_warn(
        "unable to mirror {.val {document_name}} because it was not found in raw imports"
      )
      return(character(0))
    }

    relative_paths <- fs::path_rel(matched_paths, start = raw_imports_dir)
    target_paths <- fs::path(raw_imports_mirror_dir, relative_paths)

    fs::dir_create(fs::path_dir(target_paths))
    fs::file_copy(matched_paths, target_paths, overwrite = TRUE)

    target_paths
  })

  mirrored_targets <- unlist(mirrored_targets, use.names = FALSE)

  invisible(mirrored_targets)
}

#' @title export validation audit report to excel
#' @description writes row-level validation errors to an excel workbook for
#' manual review and returns the resolved output path.
#' @param audit_dt data table containing dirty rows and `error_columns`.
#' @param output_path character scalar output path for the excel file. the
#' default is a generic project audit file and should usually be overridden by
#' `config$paths$data$audit$audit_file_path`.
#' @return character scalar with the saved excel path.
#' @importFrom checkmate assert_data_frame assert_string assert_names
#' @importFrom fs dir_create path_dir path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom cli cli_inform
#' @examples
#' # export_validation_audit_report(data.table::data.table(error_columns = "year"))
export_validation_audit_report <- function(
  audit_dt,
  output_path = fs::path(here::here("data", "audit"), "audit.xlsx")
) {
  checkmate::assert_data_frame(audit_dt)
  checkmate::assert_string(output_path, min.chars = 1)
  checkmate::assert_names(
    names(audit_dt),
    must.include = "error_columns",
    what = "names(audit_dt)"
  )

  fs::dir_create(fs::path_dir(output_path))

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "audit_report")
  openxlsx::writeData(workbook, "audit_report", audit_dt)
  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)
}

#' @title validate consolidated data for analytical readiness
#' @description validates consolidated dataset rows, exports an excel audit
#' report when dirty rows are detected, mirrors raw error imports into the
#' dataset-specific audit folder, and continues execution with a validated
#' `data.table` where `value` is numeric.
#' @param dataset_dt data frame or data table containing consolidated raw
#' observations.
#' @param config named list containing dataset-specific audit and raw import
#' paths under `paths$data`.
#' @return validated `data.table` with `value` coerced to numeric.
#' @importFrom checkmate assert_data_frame assert_list assert_directory_exists
#' @importFrom data.table as.data.table copy
#' @importFrom readr parse_double
#' @importFrom cli cli_warn
#' @examples
#' data_example <- data.frame(
#'   continent = "asia",
#'   country = "nepal",
#'   product = "rice",
#'   variable = "production",
#'   unit = "t",
#'   year = "2020",
#'   value = "1.2",
#'   notes = NA_character_,
#'   footnotes = "none",
#'   yearbook = "yb_2020",
#'   document = "sample_file.xlsx"
#' )
#' validate_data(data_example, load_pipeline_config("fao_data_raw"))
validate_data <- function(dataset_dt, config) {
  checkmate::assert_data_frame(dataset_dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_directory_exists(config$paths$data$imports$raw)

  audit_dt <- identify_validation_errors(dataset_dt)

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
  }

  validated_dt <- dataset_dt |>
    data.table::as.data.table() |>
    data.table::copy()

  validated_dt[,
    value := suppressWarnings(
      readr::parse_double(as.character(value), na = c("", "na", "nan", "null"))
    )
  ]

  validated_dt
}
