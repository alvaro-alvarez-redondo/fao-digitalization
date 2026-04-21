#' @title Standalone performance runner script
#' @description Script-level entrypoint for running performance analysis with
#'   hard-coded preset selection and optional overrides.
#' @details Source this script to execute the selected configuration
#'   immediately.
#' @keywords internal
#' @noRd
NULL

 .perf_orchestration_path <- if (requireNamespace("here", quietly = TRUE)) {
   here::here("perf", "perf_pipeline", "p9-orchestration.R")
 } else {
   file.path(getwd(), "perf", "perf_pipeline", "p9-orchestration.R")
 }
source(.perf_orchestration_path, echo = FALSE, local = FALSE)

# ── execute analysis ─────────────────────────────────────────────────────────
do.call(
  run_perf,
  build_perf_run_config(
    preset_name = "quick"
  )
)
