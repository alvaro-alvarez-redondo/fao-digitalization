# script: big_o_estimation
# description: compatibility shim — sources run_complexity_analysis.R so that
#   any existing code that sources big_o_estimation.R directly continues to
#   work without modification.
#
# NOTE: all logic now lives in the modular scripts inside this folder:
#   91-config.R, 92-synthetic_data.R, 93-complexity_models.R,
#   94-timing_harness.R, 95-benchmark_definitions.R, 96-execution_engine.R,
#   97-reporting.R, run_complexity_analysis.R
#
# Do not add new code here — edit the relevant 9x-*.R module instead.

source(
  file.path(
    tryCatch(here::here(), error = function(e) getwd()),
    "scripts", "complexity_analysis", "run_complexity_analysis.R"
  ),
  echo = FALSE, local = FALSE
)
