# script: clean and harmonize stage functions
# description: load stage-specific rule files and execute vectorized
# conditional transformations via a shared post-processing engine while
# preserving independent stage entry points.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)
}

#' @title Load cleaning rule payloads
#' @description Discovers cleaning rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list
load_cleaning_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  return(load_stage_rule_payloads(config = config, stage_name = "clean"))
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


#' @title Ensure rule-referenced columns exist in dataset
#' @description Detects missing columns referenced by canonical rules and adds
#' them by reference to the dataset as `NA_character_` before downstream
#' validation and processing.
#' @param dataset_dt Dataset to mutate by reference.
#' @param rules_dt Canonical rule table containing `column_source` and
#' `column_target`.
#' @return Invisibly returns the mutated `dataset_dt`.
#' @importFrom checkmate assert_data_table assert_data_frame
ensure_rule_columns_exist <- function(dataset_dt, rules_dt) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(rules_dt, min.rows = 0)

  if (nrow(rules_dt) == 0L) {
    return(invisible(dataset_dt))
  }

  referenced_columns <- unique(c(rules_dt$column_source, rules_dt$column_target))
  referenced_columns <- referenced_columns[!is.na(referenced_columns)]
  referenced_columns <- referenced_columns[nzchar(referenced_columns)]

  missing_columns <- setdiff(referenced_columns, colnames(dataset_dt))

  if (length(missing_columns) > 0L) {
    dataset_dt[, (missing_columns) := lapply(missing_columns, function(column_name) {
      return(NA_character_)
    })]
  }

  return(invisible(dataset_dt))
}

#' @title Run one rule-based post-processing stage
#' @description Applies one stage of rule payloads (`clean` or `harmonize`) and
#' returns transformed data with deterministic diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage name (`clean` or `harmonize`).
#' @param dataset_name Character scalar dataset identifier.
#' @return `data.table` with attributes `layer_diagnostics` and `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom data.table as.data.table copy rbindlist
#' @importFrom purrr reduce
run_rule_stage_layer_batch <- function(
  dataset_dt,
  config,
  stage_name,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(stage_name, min.chars = 1)
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

      ensure_rule_columns_exist(
        dataset_dt = state$data,
        rules_dt = canonical_rules
      )

      validate_canonical_rules(
        rules_dt = canonical_rules,
        dataset_dt = state$data,
        rule_file_id = payload$rule_file_id,
        stage_name = validated_stage_name
      )

      if (nrow(canonical_rules) == 0) {
        return(state)
      }

      payload_result <- apply_rule_payload(
        dataset_dt = state$data,
        canonical_rules = canonical_rules,
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

  stage_audit <- data.table::rbindlist(
    final_state$audit_tables,
    use.names = TRUE,
    fill = TRUE
  )

  diagnostics <- build_layer_diagnostics(
    layer_name = validated_stage_name,
    rows_in = nrow(dataset_dt),
    rows_out = nrow(final_state$data),
    audit_dt = stage_audit
  )

  stage_output_dt <- final_state$data
  attr(stage_output_dt, "layer_diagnostics") <- diagnostics
  attr(stage_output_dt, "layer_audit") <- stage_audit

  return(stage_output_dt)
}

#' @title Run cleaning layer batch
#' @description Applies clean-stage conditional rules and returns cleaned data
#' with diagnostics and audit metadata.
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

  return(run_rule_stage_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "clean",
    dataset_name = dataset_name
  ))
}

#' @title Run harmonize layer batch
#' @description Applies harmonize-stage conditional rules and returns harmonized
#' data with diagnostics and audit metadata.
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

  return(run_rule_stage_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "harmonize",
    dataset_name = dataset_name
  ))
}
