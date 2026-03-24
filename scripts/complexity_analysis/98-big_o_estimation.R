# script: 98-big_o_estimation
# description: DEPRECATED backward-compatibility shim. the complexity analysis
#   module has been redesigned and now lives in complexity_analysis/.
#   this file sources complexity_analysis/run_analysis.R which provides all
#   previously available functions including run_big_o_analysis().
#
# NOTE: this script no longer auto-executes run_big_o_analysis(). call it
# explicitly after sourcing this file.

source(
  file.path(
    tryCatch(here::here(), error = function(e) getwd()),
    "complexity_analysis", "run_analysis.R"
  ),
  echo = FALSE
)
