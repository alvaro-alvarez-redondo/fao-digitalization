# script: harmonize stage functions
# description: thin orchestration wrapper for stage-parameterized harmonize engine.

#' @title Run harmonize data stage
#' @description Executes stage-aware harmonization engine for harmonize rules.
#' @param dataset_dt Input dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset name.
#' @param rewrite_template Logical scalar template rewrite flag.
#' @return Harmonized data table with audit and diagnostics attributes.
run_harmonize_data <- function(
  dataset_dt,
  config,
  dataset_name = "fao_data_raw",
  rewrite_template = FALSE
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_flag(rewrite_template)

  return(run_stage_harmonization_engine(
    dataset_dt = dataset_dt,
    config = config,
    stage = "harmonize",
    dataset_name = dataset_name,
    rewrite_template = rewrite_template
  ))
}

# backward-compatible aliases
run_harmonization_layer_batch <- run_harmonize_data
run_standardization_layer_batch <- run_harmonize_data
