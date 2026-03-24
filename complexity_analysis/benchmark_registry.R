# complexity_analysis/benchmark_registry.R
# description: benchmark registry for the Big O complexity analysis module.
#   provides a unified catalog of pipeline-level and function-level benchmarks.
#   each entry maps to a pipeline stage (or cross-stage utility group) so that
#   no orphan metrics exist — every measurement is traceable to its origin.
#
# all benchmark closures are self-contained: they capture inputs via the
# fn_factory pattern and perform no I/O or global state mutations.
#
# depends on: workload_generators.R  (make_wide_dt, make_long_dt,
#                                     make_numeric_string_vec,
#                                     make_benchmark_config)
# sourced by:  complexity_analysis/run_analysis.R

# ── benchmark registry ───────────────────────────────────────────────────────
#
# each benchmark descriptor is a named list with fields:
#   name        : display name used in reports and file names
#   stage       : pipeline stage label for aggregation (traceability)
#   description : short description of what is measured
#   fn_factory  : function(n) → function() that runs the target operation
#
# fn_factory must be self-contained (capture everything it needs via closure)
# and must NOT perform any I/O or modify global state.

#' @title build benchmark definitions
#' @description constructs the full catalog of benchmark descriptors using the
#'   supplied config. the returned list is used by the execution engine.
#'   critical functions were selected based on frequency, runtime contribution,
#'   data-size sensitivity, and known bottlenecks in the WHEP pipeline.
#' @param cfg list returned by `get_big_o_config()`.
#' @return named list of benchmark descriptors, each with fields:
#'   name, stage, description, fn_factory.
build_benchmark_definitions <- function(cfg) {
  set.seed(cfg$rng_seed)
  bench_cfg <- make_benchmark_config()
  n_yrs     <- cfg$n_year_cols
  na_frac   <- cfg$na_fraction
  dup_frac  <- cfg$dup_fraction

  benchmarks <- list(

    # ── Stage 0: General helpers ────────────────────────────────────────────
    # Selected: cross-pipeline utilities called on every file; O(n) or worse
    # in these dominates import throughput.

    list(
      name        = "normalize_string_impl",
      stage       = "0-general",
      description = "normalise a character vector of length n to ASCII lowercase",
      fn_factory  = function(n) {
        x <- paste0(
          sample(LETTERS, n, replace = TRUE),
          sample(0:9, n, replace = TRUE),
          sample(c("é", "ñ", "ü", " ", "-", "_"), n, replace = TRUE)
        )
        function() normalize_string_impl(x)
      }
    ),

    list(
      name        = "coerce_numeric_safe",
      stage       = "0-general",
      description = "coerce a character vector of length n to numeric",
      fn_factory  = function(n) {
        x <- make_numeric_string_vec(n)
        function() coerce_numeric_safe(x)
      }
    ),

    list(
      name        = "drop_na_value_rows",
      stage       = "0-general",
      description = paste0("filter NA-value rows from a data.table of n rows (",
                            round(na_frac * 100), "% NA)"),
      fn_factory  = function(n) {
        dt <- make_long_dt(n, na_fraction = na_frac)
        function() drop_na_value_rows(data.table::copy(dt))
      }
    ),

    # ── Stage 1: Import pipeline ─────────────────────────────────────────────
    # Selected: hottest functions per profiling — reshape and consolidate
    # dominate import time for wide multi-year files.

    list(
      name        = "normalize_key_fields",
      stage       = "1-import",
      description = "normalize product/variable/continent/country in n-row wide table",
      fn_factory  = function(n) {
        df <- make_wide_dt(n, n_years = n_yrs)
        function() normalize_key_fields(data.table::copy(df), "cereals", bench_cfg)
      }
    ),

    list(
      name        = "reshape_to_long",
      stage       = "1-import",
      description = paste0("melt n-row wide table (", n_yrs,
                            " year cols) to long format"),
      fn_factory  = function(n) {
        df <- make_wide_dt(n, n_years = n_yrs)
        function() reshape_to_long(data.table::copy(df), bench_cfg)
      }
    ),

    list(
      name        = "validate_mandatory_fields_dt",
      stage       = "1-import",
      description = "check mandatory non-empty fields in n-row long table",
      fn_factory  = function(n) {
        dt <- make_long_dt(n, na_fraction = 0.05)
        function() validate_mandatory_fields_dt(data.table::copy(dt), bench_cfg)
      }
    ),

    list(
      name        = "detect_duplicates_dt",
      stage       = "1-import",
      description = paste0("detect duplicate keys in n-row long table (",
                            round(dup_frac * 100), "% dups)"),
      fn_factory  = function(n) {
        dt <- make_long_dt(n, dup_fraction = dup_frac)
        function() detect_duplicates_dt(data.table::copy(dt))
      }
    ),

    list(
      name        = "consolidate_audited_dt",
      stage       = "1-import",
      description = "consolidate and reorder columns in a list of n-row long tables",
      fn_factory  = function(n) {
        chunk    <- max(1L, n %/% 3L)
        dt_list  <- list(
          make_long_dt(chunk),
          make_long_dt(chunk),
          make_long_dt(n - 2L * chunk)
        )
        local_cfg <- bench_cfg
        function() {
          input_list <- lapply(dt_list, data.table::copy)
          consolidate_audited_dt(input_list, local_cfg)
        }
      }
    ),

    # ── Stage 2: Post-processing pipeline ────────────────────────────────────
    # Selected: aggregate_standardized_rows is a group-sum over all id columns;
    # its O(n) vs O(n log n) profile depends on duplicate density.

    list(
      name        = "aggregate_standardized_rows",
      stage       = "2-post_processing",
      description = paste0("group-sum n rows by all non-value columns (",
                            round(dup_frac * 100), "% duplicates to aggregate)"),
      fn_factory  = function(n) {
        dt_raw <- make_long_dt(n, dup_fraction = dup_frac)
        dt_raw[, value := suppressWarnings(as.numeric(value))]
        function() aggregate_standardized_rows(data.table::copy(dt_raw))
      }
    ),

    # ── Stage 3: Export pipeline ─────────────────────────────────────────────
    # Selected: compute_unique_column_values and normalize_for_comparison are
    # called once per exported column/table and scale with dataset size.

    list(
      name        = "compute_unique_column_values",
      stage       = "3-export",
      description = "compute sorted unique values for one column of n-row table",
      fn_factory  = function(n) {
        dt <- make_long_dt(n)
        function() compute_unique_column_values(dt, "country")
      }
    ),

    list(
      name        = "normalize_for_comparison",
      stage       = "3-export",
      description = "deep-copy, drop year col, sort columns and rows of n-row table",
      fn_factory  = function(n) {
        dt <- make_long_dt(n)
        function() normalize_for_comparison(data.table::copy(dt))
      }
    )
  )

  names(benchmarks) <- vapply(benchmarks, function(b) b$name, character(1))
  return(benchmarks)
}
