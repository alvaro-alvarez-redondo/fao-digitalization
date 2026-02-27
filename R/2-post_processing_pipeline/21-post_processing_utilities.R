# script: common clean-harmonize helper functions
# description: shared diagnostics and rule-table helpers used by cleaning,
# harmonization, and numeric harmonization stages.


#' @title build standardized layer diagnostics object
#' @description create a one-row diagnostics object following the phase-1
#' contract.
#' @param layer_name character scalar layer name.
#' @param rows_in integer scalar row count before layer.
#' @param rows_out integer scalar row count after layer.
#' @param matched_count integer scalar.
#' @param unmatched_count integer scalar.
#' @param idempotence_passed logical scalar.
#' @param validation_passed logical scalar.
#' @param status character scalar one of `pass`, `warn`, `fail`.
#' @param messages character vector of diagnostics messages.
#' @return named list diagnostics object.
#' @importFrom checkmate assert_string assert_int assert_flag assert_choice assert_character
#' @examples
#' build_layer_diagnostics("cleaning", rows_in = 10L, rows_out = 10L)
build_layer_diagnostics <- function(
  layer_name,
  rows_in,
  rows_out,
  matched_count = 0L,
  unmatched_count = 0L,
  idempotence_passed = TRUE,
  validation_passed = TRUE,
  status = "pass",
  messages = character(0)
) {
  checkmate::assert_string(layer_name, min.chars = 1)
  checkmate::assert_int(rows_in, lower = 0)
  checkmate::assert_int(rows_out, lower = 0)
  checkmate::assert_int(matched_count, lower = 0)
  checkmate::assert_int(unmatched_count, lower = 0)
  checkmate::assert_flag(idempotence_passed)
  checkmate::assert_flag(validation_passed)
  checkmate::assert_choice(status, c("pass", "warn", "fail"))
  checkmate::assert_character(messages, any.missing = FALSE)

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(
      Sys.time(),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    ),
    rows_in = as.integer(rows_in),
    rows_out = as.integer(rows_out),
    matched_count = as.integer(matched_count),
    unmatched_count = as.integer(unmatched_count),
    idempotence_passed = idempotence_passed,
    validation_passed = validation_passed,
    status = status,
    messages = messages
  )

  return(diagnostics)
}


#' @title persist diagnostics artifact as csv
#' @description write one diagnostics csv file under `data/audit/<dataset>/`.
#' @param diagnostics diagnostics list from `build_layer_diagnostics()`.
#' @param config named config list containing audit path fields.
#' @return character scalar output file path.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
#' @importFrom readr write_csv
#' @examples
#' \dontrun{persist_layer_diagnostics(build_layer_diagnostics("cleaning", rows_in = 1L, rows_out = 1L), config)}
persist_layer_diagnostics <- function(diagnostics, config) {
  checkmate::assert_list(diagnostics, min.len = 1)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$audit$audit_dir, min.chars = 1)

  audit_dir <- config$paths$data$audit$audit_dir
  fs::dir_create(audit_dir, recurse = TRUE)

  file_name <- paste0(
    diagnostics$layer_name,
    "_diagnostics_",
    format(Sys.time(), "%Y%m%d_%H%M%S", tz = "UTC"),
    ".csv"
  )

  output_path <- fs::path(audit_dir, file_name)

  diagnostics_dt <- data.table::data.table(
    layer_name = diagnostics$layer_name,
    execution_timestamp_utc = diagnostics$execution_timestamp_utc,
    rows_in = diagnostics$rows_in,
    rows_out = diagnostics$rows_out,
    matched_count = diagnostics$matched_count,
    unmatched_count = diagnostics$unmatched_count,
    idempotence_passed = diagnostics$idempotence_passed,
    validation_passed = diagnostics$validation_passed,
    status = diagnostics$status,
    messages = paste(diagnostics$messages, collapse = " | ")
  )

  readr::write_csv(diagnostics_dt, output_path)

  return(output_path)
}


#' @title read rule table from csv or xlsx
#' @description load a mapping table from a file path using extension-specific
#' readers.
#' @param file_path character scalar path to rule file.
#' @return data.table rule table.
#' @importFrom checkmate assert_string assert_file_exists
#' @importFrom fs path_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @examples
#' \dontrun{read_rule_table("data/imports/cleaning imports/cleaning_rules.csv")}
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  extension <- fs::path_ext(file_path) |>
    tolower()

  if (extension == "csv") {
    layer_rules <- readr::read_csv(file_path, show_col_types = FALSE)
    return(data.table::as.data.table(layer_rules))
  }

  if (extension %in% c("xlsx", "xls")) {
    layer_rules <- readxl::read_excel(file_path)
    return(data.table::as.data.table(layer_rules))
  }

  cli::cli_abort("unsupported rule file extension for {.file {file_path}}")
}


#' @title validate required rule schema
#' @description enforce required columns and missingness checks for rule tables.
#' @param layer_rules data.table rule table.
#' @param required_columns character vector of required columns.
#' @param layer_name character scalar layer name for messages.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character assert_string
#' @examples
#' validate_rule_schema(data.table::data.table(a = 1), "a", "example")
validate_rule_schema <- function(layer_rules, required_columns, layer_name) {
  checkmate::assert_data_frame(layer_rules, min.rows = 1)
  checkmate::assert_character(
    required_columns,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_string(layer_name, min.chars = 1)

  missing_columns <- setdiff(required_columns, colnames(layer_rules))

  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "{layer_name} rule schema is missing required columns",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  required_subset <- layer_rules[, ..required_columns]

  for (column_name in required_columns) {
    if (any(is.na(required_subset[[column_name]]))) {
      cli::cli_abort(
        "{layer_name} rule table contains missing values in required column {.val {column_name}}"
      )
    }
  }

  return(invisible(TRUE))
}


#' @title validate mapping rules
#' @description validate schema, active-key uniqueness, and target column
#' availability for cleaning mappings.
#' @param layer_rules data.table cleaning rules.
#' @param column_targets character vector of dataset columns to clean.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character
#' @examples
#' \dontrun{validate_mapping_rules(layer_rules, c("country", "unit"))}
validate_mapping_rules <- function(
  layer_rules,
  dataset_columns = NULL,
  value_column = c("cleaned_value_target", "harmonized_value_target"),
  layer_name = "mapping"
) {
  # value_column can be "cleaned_value_target" or "harmonized_value_target"
  value_column <- match.arg(value_column)

  checkmate::assert_data_frame(layer_rules, min.rows = 1)

  # Required columns
  required_columns <- c(
    "column_source",
    "column_target",
    "original_value_source",
    "original_value_target",
    value_column
  )

  # Validate schema
  validate_rule_schema(layer_rules, required_columns, layer_name)

  layer_rules <- data.table::as.data.table(layer_rules)

  # Check for duplicates
  duplicate_active <- layer_rules[,
    .N,
    by = .(
      column_source,
      column_target,
      original_value_source,
      original_value_target
    )
  ][N > 1]

  if (nrow(duplicate_active) > 0) {
    cli::cli_abort(c(
      "{layer_name} rules contain duplicate active mappings",
      "x" = "Each (column_source, column_target, original_value_source, original_value_target) must be unique"
    ))
  }

  # Warn about unknown columns in dataset
  if (!is.null(dataset_columns)) {
    unknown_sources <- setdiff(
      unique(layer_rules$column_source),
      dataset_columns
    )
    if (length(unknown_sources) > 0) {
      cli::cli_warn(c(
        "{layer_name} rules contain source columns not present in dataset",
        "i" = paste(unknown_sources, collapse = ", ")
      ))
    }

    unknown_targets <- setdiff(
      unique(layer_rules$column_target),
      dataset_columns
    )
    if (length(unknown_targets) > 0) {
      cli::cli_warn(c(
        "{layer_name} rules contain target columns not present in dataset",
        "i" = paste(unknown_targets, collapse = ", ")
      ))
    }
  }

  return(invisible(TRUE))
}

