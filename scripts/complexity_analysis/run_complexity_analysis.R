# scripts/complexity_analysis/run_complexity_analysis.R
# description: backward-compatible entrypoint shim. sources the new
#   complexity_analysis/ module which exposes run_big_o_analysis() and all
#   supporting functions. does NOT auto-execute the analysis — call
#   run_big_o_analysis() explicitly after sourcing this file.
#
# for a full analysis run with default settings:
#   source(here::here("scripts", "complexity_analysis", "run_complexity_analysis.R"))
#   run_big_o_analysis()

source(
  here::here("complexity_analysis", "run_analysis.R"),
  echo = FALSE
)
