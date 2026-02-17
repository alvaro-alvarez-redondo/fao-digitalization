# script:
# description:

#' @title identify row-level audit errors for consolidated data
#' @description subset consolidated `fao_data_raw` rows that have missing values
#' in mandatory audit key columns. this simplified audit only checks `continent`,
#' `country`, and `product` for `na` values.
#' @param fao_data_raw data frame or data table containing consolidated raw fao
#' observations.
#' @return `data.table` containing only rows where at least one of
#' `continent`, `country`, or `product` is `na`, sorted alphabetically by
#' `document`.
#' @importFrom checkmate assert_data_frame assert_names assert_character
#' @importFrom data.table as.data.table copy setorderv
#' @importFrom purrr walk
#' @examples
#' data_example <- data.frame(
#'   continent = c("asia", NA_character_),
#'   country = c("nepal", "nepal"),
#'   product = c("rice", "rice"),
#'   document = c("clean_file.xlsx", "dirty_file.xlsx")
#' )
#' identify_audit_errors(data_example, config)
identify_audit_errors <- function(fao_data_raw, config) {
  checkmate::assert_data_frame(fao_data_raw)

  audit_columns <- config$audit_columns

  checkmate::assert_names(
    names(fao_data_raw),
    must.include = config$column_order,
    what = "names(fao_data_raw)"
  )

  purrr::walk(
    audit_columns,
    \(column_name) {
      checkmate::assert_character(fao_data_raw[[column_name]])
    }
  )

  audit_dt <- fao_data_raw |>
    data.table::as.data.table() |>
    data.table::copy()

  row_has_missing_audit_values <- audit_dt[,
    rowSums(is.na(.SD)) > 0,
    .SDcols = audit_columns
  ]

  output_dt <- audit_dt[row_has_missing_audit_values]
  data.table::setorderv(output_dt, cols = "document", na.last = TRUE)

  output_dt
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
#' manual review and returns the resolved output path. before export, rows are
#' sorted alphabetically by `document`.
#' @param audit_dt data table containing dirty rows.
#' @param output_path character scalar output path for the excel file. the
#' default is a generic project audit file and should usually be overridden by
#' `config$paths$data$audit$audit_file_path`.
#' @return character scalar with the saved excel path.
#' @importFrom checkmate assert_data_frame assert_string assert_names
#' @importFrom fs dir_create path_dir path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom cli cli_inform
#' @examples
#' # export_validation_audit_report(data.table::data.table(document = "sample.xlsx"))
export_validation_audit_report <- function(
  audit_dt,
  output_path = fs::path(here::here("data", "audit"), "audit.xlsx")
) {
  checkmate::assert_data_frame(audit_dt)
  checkmate::assert_string(output_path, min.chars = 1)
  checkmate::assert_names(
    names(audit_dt),
    must.include = "document",
    what = "names(audit_dt)"
  )

  export_dt <- audit_dt |>
    data.table::as.data.table() |>
    data.table::copy()

  data.table::setorderv(export_dt, cols = "document", na.last = TRUE)

  fs::dir_create(fs::path_dir(output_path))

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "audit_report")
  openxlsx::writeData(workbook, "audit_report", export_dt)
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
#' audit_data_output(data_example, load_pipeline_config("fao_data_raw"))
audit_data_output <- function(dataset_dt, config) {
  checkmate::assert_data_frame(dataset_dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_directory_exists(config$paths$data$imports$raw)

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
  }

  audited_dt <- dataset_dt |>
    data.table::as.data.table() |>
    data.table::copy()

  audited_dt[,
    value := suppressWarnings(
      readr::parse_double(as.character(value), na = c("", "na", "nan", "null"))
    )
  ]

  audited_dt
}
