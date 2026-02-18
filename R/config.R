# script: central pipeline configuration
# description: single source of truth for global pipeline metadata, audit schema,
# and runtime requirements.

#' @title central pipeline configuration
#' @description this script defines a standalone `config` object that can be
#' sourced at pipeline start. it centralizes input/output requirements, audit
#' schema metadata, and system-level settings.

# input requirements for raw excel sheets.
input_required_columns <- c("continent", "country", "unit", "footnotes")

# output column order used by validate_output_column_order().
output_column_order <- c(
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

# canonical audit findings schema derived from empty_audit_findings_dt() logic.
audit_canonical_schema <- data.table::data.table(
  column_name = c(
    "row_index",
    "audit_column",
    "audit_type",
    "audit_message"
  ),
  column_type = c(
    "integer",
    "character",
    "character",
    "character"
  )
)

# system-level runtime settings.
system_settings <- list(
  excel_process_name = "EXCEL.EXE",
  log_path = here::here("logs", "pipeline.log")
)

# centralized config object.
config <- list(
  input = list(required_columns = input_required_columns),
  output = list(column_order = output_column_order),
  audit = list(canonical_schema = audit_canonical_schema),
  system = system_settings
)

# config validator block.
checkmate::assert_list(config, min.len = 4, names = "named")
checkmate::assert_names(
  names(config),
  must.include = c("input", "output", "audit", "system")
)

checkmate::assert_character(
  config$input$required_columns,
  min.len = 1,
  any.missing = FALSE,
  unique = TRUE
)

checkmate::assert_character(
  config$output$column_order,
  min.len = 1,
  any.missing = FALSE,
  unique = TRUE
)

checkmate::assert_data_frame(config$audit$canonical_schema, min.rows = 1)
checkmate::assert_names(
  names(config$audit$canonical_schema),
  must.include = c("column_name", "column_type")
)
checkmate::assert_character(
  config$audit$canonical_schema$column_name,
  min.len = 1,
  any.missing = FALSE,
  unique = TRUE
)
checkmate::assert_character(
  config$audit$canonical_schema$column_type,
  len = nrow(config$audit$canonical_schema),
  any.missing = FALSE
)

expected_schema_names <- c(
  "row_index",
  "audit_column",
  "audit_type",
  "audit_message"
)

if (exists("empty_audit_findings_dt", mode = "function")) {
  expected_schema_names <- names(empty_audit_findings_dt())
}

checkmate::assert_subset(
  expected_schema_names,
  config$audit$canonical_schema$column_name
)

checkmate::assert_string(config$system$excel_process_name, min.chars = 1)
checkmate::assert_string(config$system$log_path, min.chars = 1)
