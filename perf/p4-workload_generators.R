# module:      p4-workload_generators
# description: stage-organised benchmark descriptor catalogue. benchmark
#   closures are self-contained: they capture all inputs via the fn_factory
#   pattern and perform no I/O or global state mutations. functions are grouped
#   by pipeline stage so the execution engine can run each stage independently.
#
# depends on:  p1-synthetic_data.R
# sourced by:  perf/run_perf.R

# ── 5. workload generators ────────────────────────────────────────────────────
#
# each benchmark descriptor is a named list with:
#   name        : unique identifier (used for reporting and plot file names)
#   stage       : pipeline stage label (one of "0-general", "1-import",
#                 "2-post_processing", "3-export")
#   description : one-line summary of what is measured
#   fn_factory  : function(n) → function() that runs the target operation once
#
# fn_factory must be self-contained (capture everything via closure) and must
# NOT perform any I/O or modify global state.

# ── 5a. stage 0 — general pipeline ──────────────────────────────────────────

.build_stage_0_general_benchmarks <- function(cfg) {
  na_frac <- cfg$na_fraction
  list(
    list(
      name        = "normalize_string_impl",
      stage       = "0-general",
      description = "normalise a character vector of length n to ASCII lowercase",
      fn_factory  = function(n) {
        x <- paste0(
          sample(LETTERS, n, replace = TRUE),
          sample(0:9,     n, replace = TRUE),
          sample(c("\u00e9", "\u00f1", "\u00fc", " ", "-", "_"), n, replace = TRUE)
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
                            round(na_frac * 100L), "% NA)"),
      fn_factory  = function(n) {
        dt <- make_long_dt(n, na_fraction = na_frac)
        function() drop_na_value_rows(data.table::copy(dt))
      }
    )
  )
}

# ── 5b. stage 1 — import pipeline ────────────────────────────────────────────

.build_stage_1_import_benchmarks <- function(cfg) {
  n_yrs    <- cfg$n_year_cols
  dup_frac <- cfg$dup_fraction
  bench_cfg <- make_benchmark_config()

  list(
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
                            round(dup_frac * 100L), "% dups)"),
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
    )
  )
}

# ── 5c. stage 2 — post-processing pipeline ───────────────────────────────────

.build_stage_2_post_processing_benchmarks <- function(cfg) {
  dup_frac <- cfg$dup_fraction

  list(
    list(
      name        = "aggregate_standardized_rows",
      stage       = "2-post_processing",
      description = paste0("group-sum n rows by all non-value columns (",
                            round(dup_frac * 100L), "% duplicates to aggregate)"),
      fn_factory  = function(n) {
        dt_raw <- make_long_dt(n, dup_fraction = dup_frac)
        dt_raw[, value := suppressWarnings(as.numeric(value))]
        function() aggregate_standardized_rows(data.table::copy(dt_raw))
      }
    )
  )
}

# ── 5d. stage 3 — export pipeline ────────────────────────────────────────────

.build_stage_3_export_benchmarks <- function(cfg) {
  list(
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
}

# ── 5e. public builders ───────────────────────────────────────────────────────

#' @title build benchmark descriptors for a single pipeline stage
#' @description returns benchmark descriptors for the requested pipeline stage
#'   using parameters from `cfg`.
#' @param stage_id character scalar — one of "0-general", "1-import",
#'   "2-post_processing", "3-export".
#' @param cfg named list from `get_analysis_config()`.
#' @return named list of benchmark descriptors for the requested stage.
build_stage_benchmarks <- function(stage_id, cfg) {
  benchmarks <- switch(stage_id,
    "0-general"        = .build_stage_0_general_benchmarks(cfg),
    "1-import"         = .build_stage_1_import_benchmarks(cfg),
    "2-post_processing" = .build_stage_2_post_processing_benchmarks(cfg),
    "3-export"         = .build_stage_3_export_benchmarks(cfg),
    stop(sprintf("unknown stage_id: '%s'", stage_id))
  )
  names(benchmarks) <- vapply(benchmarks, function(b) b$name, character(1))
  return(benchmarks)
}

#' @title build all benchmark descriptors
#' @description returns the complete catalogue of benchmark descriptors across
#'   all pipeline stages. this is the primary entry point used by the execution
#'   engine when running a flat (non-staged) analysis.
#' @param cfg named list from `get_analysis_config()`.
#' @return named list of all benchmark descriptors.
build_benchmark_definitions <- function(cfg) {
  set.seed(cfg$rng_seed)
  all_benchmarks <- lapply(cfg$stages, build_stage_benchmarks, cfg = cfg)
  combined <- do.call(c, all_benchmarks)
  names(combined) <- vapply(combined, function(b) b$name, character(1))
  return(combined)
}

#' @title build all benchmarks (alias)
#' @description alias for `build_benchmark_definitions()`, provided for
#'   symmetry with `build_stage_benchmarks()`.
#' @param cfg named list from `get_analysis_config()`.
#' @return named list of all benchmark descriptors.
build_all_benchmarks <- function(cfg) build_benchmark_definitions(cfg)
