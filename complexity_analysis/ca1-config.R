# module:      ca1-config
# description: analysis configuration for the complexity analysis module.
#   defines get_analysis_config(), the single source of truth for all tunable
#   parameters used across all analysis stages. get_big_o_config() is provided
#   as a backward-compatible alias so existing callers are unaffected.
#
# sourced by:  complexity_analysis/run_complexity_analysis.R

# ── 1. analysis configuration ────────────────────────────────────────────────

#' @title default complexity analysis configuration
#' @description returns the default parameter set for the full complexity
#'   analysis. covers input sizing, replication count, per-stage workload
#'   tuning, output paths, and display toggles. override individual fields
#'   with `utils::modifyList()` to customise a run without rebuilding the
#'   entire config.
#' @return named list of analysis parameters.
get_analysis_config <- function() {
  list(
    # input sizes (n) to benchmark over — logarithmically spaced so complexity
    # differences are clearly visible across candidate model fits.
    input_sizes = c(100L, 500L, 1000L, 2500L, 5000L, 10000L, 25000L, 50000L),

    # replications per (function, n) cell. more reps → lower measurement noise.
    n_reps = 5L,

    # number of year columns in synthetic wide-format tables (import stage)
    n_year_cols = 10L,

    # fraction of rows with NA value column (for na-filter benchmarks)
    na_fraction = 0.2,

    # fraction of rows that are exact duplicates (for duplicate-detection benchmarks)
    dup_fraction = 0.1,

    # set.seed value for full reproducibility
    rng_seed = 42L,

    # stages to analyse — order determines report presentation
    stages = c("0-general", "1-import", "2-post_processing", "3-export"),

    # number of top bottleneck functions to highlight per stage
    top_n_bottlenecks = 3L,

    # base directory for JSON and plot output files. NULL → tempdir() subfolder
    output_dir = NULL,

    # path for the persisted full analysis object (.qs format)
    qs_path = file.path(
      "data", "2-post_processing",
      "post_processing_diagnostics",
      "complexity_analysis_complete.qs"
    ),

    # whether to produce PNG plot files
    produce_plots = TRUE,

    # suppress progress messages and progress bars
    quiet = FALSE
  )
}

#' @title backward-compatible alias for get_analysis_config
#' @description returns the default analysis configuration. this alias exists
#'   so that code and tests written against the previous `get_big_o_config()`
#'   API continue to work without modification.
#' @return named list of analysis parameters (see `get_analysis_config()`).
get_big_o_config <- function() get_analysis_config()
