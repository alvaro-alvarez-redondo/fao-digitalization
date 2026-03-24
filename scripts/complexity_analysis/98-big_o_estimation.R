# backward-compat shim: sources the top-level complexity_analysis module.
# the production entry point is complexity_analysis/run_analysis.R.
source(
  here::here("complexity_analysis", "run_analysis.R"),
  echo = FALSE
)
