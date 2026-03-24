# module: config
# description: analysis configuration for the complexity analysis module.
#   defines get_big_o_config(), the single source of truth for all tunable
#   parameters used by the benchmarking and reporting stages.
#
# sourced by: run_analysis.R

# ── configuration ─────────────────────────────────────────────────────────────

#' @title default Big O analysis configuration
#' @description returns the default parameter set for the Big O analysis.
#'   override individual fields to customise input sizes, repetitions, etc.
#' @return named list of analysis parameters.
get_big_o_config <- function() {
  list(
    # input sizes (n) to benchmark over — logarithmically spaced so complexity
    # differences are clearly visible in plots
    input_sizes = c(100L, 500L, 1000L, 2500L, 5000L, 10000L, 25000L, 50000L),

    # number of replications per (function, n) pair. more reps → lower noise.
    n_reps = 5L,

    # number of year columns in synthetic wide-format tables
    n_year_cols = 10L,

    # fraction of rows that have NA values in value column (for na-filter tests)
    na_fraction = 0.2,

    # fraction of rows that are exact duplicates (for duplicate-detection tests)
    dup_fraction = 0.1,

    # set.seed value for reproducibility
    rng_seed = 42L,

    # directory for output files (JSON + plots). NULL → current tempdir.
    output_dir = NULL,

    # whether to produce plot files
    produce_plots = TRUE,

    # whether to suppress progress messages
    quiet = FALSE
  )
}
