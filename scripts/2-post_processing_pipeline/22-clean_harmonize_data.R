# script: clean and harmonize stage functions
# description: shared execution utilities for clean and harmonize post-processing
# stages with deterministic payload handling and audit output.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)
}

#' @title Run one post-processing layer batch
#' @description Executes one layer (`clean` or `harmonize`) using shared logic:
#' rule loading, normalization, ensured referenced columns, validation, and
#' deterministic rule application with audit aggregation.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage label (`clean` or `harmonize`).
#' @param dataset_name Character scalar dataset identifier.
#' @return `data.table` with attributes `layer_diagnostics` and `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom purrr reduce
run_post_processing_layer_batch <- function(
  dataset_dt,
  config,
  stage_name,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  payloads <- load_stage_rule_payloads(
    config = config,
    stage_name = validated_stage_name
  )
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
        stage_name = validated_stage_name,
        rule_file_id = payload$rule_file_id
      )

      normalized_data <- normalize_dataset_column_names(state$data)
      normalized_rules <- normalize_rule_columns(canonical_rules)

      ensure_rule_columns_exist(
        rules_dt = normalized_rules,
        dataset_dt = normalized_data
      )

      validate_canonical_rules(
        rules_dt = normalized_rules,
        dataset_dt = normalized_data,
        rule_file_id = payload$rule_file_id,
        stage_name = validated_stage_name
      )

      if (nrow(normalized_rules) == 0L) {
        state$data <- normalized_data
        return(state)
      }

      payload_result <- apply_rule_payload(
        dataset_dt = normalized_data,
        canonical_rules = normalized_rules,
        stage_name = validated_stage_name,
        dataset_name = dataset_name,
        rule_file_id = payload$rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      state$data <- payload_result$data
      state$audit_tables[[length(state$audit_tables) + 1L]] <- payload_result$audit

      return(state)
    }
  )

  combined_audit <- data.table::rbindlist(final_state$audit_tables, use.names = TRUE, fill = TRUE)

  diagnostics <- build_layer_diagnostics(
    layer_name = validated_stage_name,
    rows_in = nrow(dataset_dt),
    rows_out = nrow(final_state$data),
    audit_dt = combined_audit
  )

  output_dt <- final_state$data
  attr(output_dt, "layer_diagnostics") <- diagnostics
  attr(output_dt, "layer_audit") <- combined_audit

  return(output_dt)
}

#' @title Load cleaning rule payloads
#' @description Discovers clean rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list
load_cleaning_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  return(load_stage_rule_payloads(config = config, stage_name = "clean"))
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
run_cleaning_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_post_processing_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "clean",
    dataset_name = dataset_name
  ))
}

#' @title Load harmonize rule payloads
#' @description Discovers harmonize rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list
load_harmonize_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  return(load_stage_rule_payloads(config = config, stage_name = "harmonize"))
}

#' @title Run harmonize layer batch
#' @description Applies harmonize-stage conditional harmonization rules and
#' returns harmonized data with diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Harmonized `data.table` with attributes `layer_diagnostics` and
#' `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_harmonize_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_post_processing_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "harmonize",
    dataset_name = dataset_name
  ))
}

# backward-compatible aliases
run_harmonization_layer_batch <- run_harmonize_layer_batch
