# backward-compatibility shim — delegates to the canonical module.
# the complexity analysis framework has been redesigned as a top-level module
# at complexity_analysis/. this script sources the new entry-point so that any
# caller that previously sourced scripts/complexity_analysis/98-big_o_estimation.R
# continues to work without modification.
source(
  here::here("complexity_analysis", "run_complexity_analysis.R"),
  echo = FALSE
)
