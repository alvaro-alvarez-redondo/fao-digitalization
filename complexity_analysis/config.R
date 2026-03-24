# complexity_analysis/config.R
# description: centralised configuration layer for the Big O complexity
#   analysis module. exposes get_big_o_config() as the single source of truth
#   for all tunable parameters used across benchmarking, timing, and reporting.
#
# sourced by: complexity_analysis/run_analysis.R

# ── configuration layer ──────────────────────────────────────────────────────

#' @title default Big O analysis configuration
#' @description returns the default parameter set for the Big O analysis.
#'   override individual fields to customise input sizes, repetitions, etc.
#'   every downstream module must derive its settings from this object so that
#'   the full analysis run is deterministic and traceable.
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

    # directory for JSON + plot outputs. NULL → tempdir()/big_o_analysis
    output_dir = NULL,

    # whether to produce plot files
    produce_plots = TRUE,

    # whether to suppress progress messages
    quiet = FALSE
  )
}
