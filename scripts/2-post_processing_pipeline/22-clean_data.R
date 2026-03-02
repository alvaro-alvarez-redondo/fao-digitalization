# script: cleaning stage functions
# description: load clean-stage rule files and execute vectorized conditional
# harmonization via shared post-processing engine.

#' @title Load cleaning rule payloads
#' @description Discovers cleaning rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path_file
#' @importFrom purrr map
load_cleaning_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$cleaning, min.chars = 1)

  cleaning_dir <- config$paths$data$imports$cleaning
  fs::dir_create(cleaning_dir, recurse = TRUE)

  rule_files <- fs::dir_ls(
    path = cleaning_dir,
    regexp = "^cleaning_.*\\.(xlsx|xls|csv)$",
    type = "file"
  )

  ordered_files <- sort(rule_files)

  payloads <- purrr::map(ordered_files, function(file_path) {
    list(
      rule_file_id = fs::path_file(file_path),
      raw_rules = read_rule_table(file_path)
    )
  })

  return(payloads)
}

#' @title Run cleaning layer batch
#' @description Applies clean-stage conditional harmonization rules and returns
#' cleaned data with structured diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Cleaned `data.table` with attributes `layer_diagnostics` and
#' `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom purrr reduce
run_cleaning_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = "fao_data_raw"
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  payloads <- load_cleaning_rule_payloads(config)
  execution_timestamp_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  initial_state <- list(
    data = data.table::copy(data.table::as.data.table(dataset_dt)),
    audit_tables = list()
  )

  final_state <- purrr::reduce(
    .x = payloads,
    .init = initial_state,
    .f = function(state, payload) {
      canonical_rules <- coerce_rule_schema(
        rule_dt = payload$raw_rules,
        stage_name = "clean",
        rule_file_id = payload$rule_file_id
      )

      validate_canonical_rules(
        rules_dt = canonical_rules,
        dataset_dt = state$data,
        rule_file_id = payload$rule_file_id
      )

      if (nrow(canonical_rules) == 0) {
        return(state)
      }

      payload_result <- apply_rule_payload(
        dataset_dt = state$data,
        canonical_rules = canonical_rules,
        stage_name = "clean",
        dataset_name = dataset_name,
        rule_file_id = payload$rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      state$data <- payload_result$data
      state$audit_tables[[length(state$audit_tables) + 1L]] <- payload_result$audit

      return(state)
    }
  )

  clean_audit <- data.table::rbindlist(final_state$audit_tables, use.names = TRUE, fill = TRUE)
  diagnostics <- build_layer_diagnostics(
    layer_name = "clean",
    rows_in = nrow(dataset_dt),
    rows_out = nrow(final_state$data),
    audit_dt = clean_audit
  )

  cleaned_dt <- final_state$data
  attr(cleaned_dt, "layer_diagnostics") <- diagnostics
  attr(cleaned_dt, "layer_audit") <- clean_audit

  return(cleaned_dt)
}
