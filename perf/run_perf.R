#' @title Standalone performance runner script
#' @description Script-level entrypoint for running performance analysis with
#'   hard-coded preset selection and optional overrides.
#' @details Source this script to execute the selected configuration
#'   immediately and assign the output to result.
#' @keywords internal
#' @noRd
NULL

.perf_orchestration_path <- tryCatch(
  here::here("perf", "p9-orchestration.R"),
  error = function(e) file.path(getwd(), "perf", "p9-orchestration.R")
)
source(.perf_orchestration_path, echo = FALSE, local = FALSE)

# ── user-editable main configuration ─────────────────────────────────────────
perf_cfg <- build_perf_run_config(
  preset_name = "standard"
)

# ── execute analysis ─────────────────────────────────────────────────────────
result <- run_perf(
  input_sizes = perf_cfg$input_sizes,
  n_reps = perf_cfg$n_reps,
  n_year_cols = perf_cfg$n_year_cols,
  stages = perf_cfg$stages,
  na_fraction = perf_cfg$na_fraction,
  dup_fraction = perf_cfg$dup_fraction,
  rng_seed = perf_cfg$rng_seed,
  output_dir = perf_cfg$output_dir,
  quiet = perf_cfg$quiet
)
