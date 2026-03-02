# script: standardization stage functions
# description: load standardization rule files and execute vectorized
# conditional harmonization via shared post-processing engine.

#' @title Load standardization rule payloads
#' @description Discovers harmonization rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path_file
#' @importFrom purrr map
load_standardization_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$harmonization, min.chars = 1)

  harmonization_dir <- config$paths$data$imports$harmonization
  fs::dir_create(harmonization_dir, recurse = TRUE)

  rule_files <- fs::dir_ls(
    path = harmonization_dir,
    regexp = "^harmonization_.*\\.(xlsx|xls|csv)$",
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

#' @title Run standardization layer batch
#' @description Applies standardize-stage conditional harmonization rules and
#' returns standardized data with diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Standardized `data.table` with attributes `layer_diagnostics` and
#' `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom purrr reduce
run_harmonization_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = "fao_data_raw"
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  payloads <- load_standardization_rule_payloads(config)
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
        stage_name = "standardize",
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
        stage_name = "standardize",
        dataset_name = dataset_name,
        rule_file_id = payload$rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      state$data <- payload_result$data
      state$audit_tables[[length(state$audit_tables) + 1L]] <- payload_result$audit

      return(state)
    }
  )

  standardize_audit <- data.table::rbindlist(final_state$audit_tables, use.names = TRUE, fill = TRUE)
  diagnostics <- build_layer_diagnostics(
    layer_name = "standardize",
    rows_in = nrow(dataset_dt),
    rows_out = nrow(final_state$data),
    audit_dt = standardize_audit
  )

  standardized_dt <- final_state$data
  attr(standardized_dt, "layer_diagnostics") <- diagnostics
  attr(standardized_dt, "layer_audit") <- standardize_audit

  return(standardized_dt)
}

# backward-compatible alias
run_standardization_layer_batch <- run_harmonization_layer_batch
