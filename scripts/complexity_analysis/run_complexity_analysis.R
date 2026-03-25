# backward-compatibility shim — delegates to the canonical module.
# the complexity analysis framework has been redesigned as a top-level module
# at complexity_analysis/. this script sources the new entry-point so that any
# caller that previously sourced scripts/complexity_analysis/run_complexity_analysis.R
# continues to work without modification. all public symbols (get_big_o_config,
# run_big_o_analysis, make_benchmark_config, etc.) are provided by the module.
source(
  here::here("complexity_analysis", "run_complexity_analysis.R"),
  echo = FALSE
)
