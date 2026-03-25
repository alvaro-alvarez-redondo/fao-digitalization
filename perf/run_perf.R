# run_perf.R
# ─────────────────────────────────────────────────────────────────────────────
# WHEP Pipeline — Performance Analysis
#
# HOW TO USE:
#   1. Adjust the settings below to your needs.
#   2. Source this file or run it line-by-line in RStudio.
#
# All internal engine code lives in p8-variables.R (sourced automatically).
# ─────────────────────────────────────────────────────────────────────────────

source(here::here("perf", "p8-variables.R"))

# ── settings (edit these) ─────────────────────────────────────────────────────

# number of repeated measurements per (function, input-size) cell.
# higher → less noise, longer runtime.  recommended range: 3 – 20.
n_reps <- 5L

# input sizes (number of rows) to benchmark at.
# logarithmic spacing makes complexity differences easy to spot.
input_sizes <- c(100L, 500L, 1000L, 2500L, 5000L, 10000L, 25000L, 50000L)

# pipeline stages to include in the analysis.
# remove any stage you do not want benchmarked.
stages <- c("0-general", "1-import", "2-post_processing", "3-export")

# set TRUE to save PNG plots alongside the JSON report.
write_plots <- TRUE

# set TRUE to silence all progress messages.
quiet <- FALSE

# ── run ───────────────────────────────────────────────────────────────────────

result <- run_big_o_analysis(
  cfg = utils::modifyList(
    get_analysis_config(),
    list(
      n_reps      = n_reps,
      input_sizes = input_sizes,
      stages      = stages
    )
  ),
  write_plots = write_plots,
  quiet       = quiet
)
