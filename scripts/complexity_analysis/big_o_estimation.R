# script: big_o_estimation
# description: standalone empirical Big O (time complexity) analysis module for
#   the WHEP digitalization pipeline. estimates the computational complexity of
#   each pipeline stage by timing key functions over synthetic inputs of
#   increasing size, fitting timing data to standard complexity models, and
#   reporting actionable results with plots.
#
# IMPORTANT: this script is fully independent from the main pipeline. it
#   sources only the general-purpose helper scripts (read-only, no I/O side
#   effects) and generates all inputs synthetically. it never modifies, reads,
#   or writes any production data files.
#
# usage:
#   source("scripts/complexity_analysis/big_o_estimation.R")
#   results <- run_big_o_analysis()              # full analysis
#   results <- run_big_o_analysis(output_dir = "/tmp/big_o_out")  # custom dir
#
# outputs:
#   - console table with complexity class per function and dominant stage
#   - JSON summary file  (<output_dir>/big_o_summary.json)
#   - per-function runtime plots  (<output_dir>/plots/)

# ── 0. bootstrap: disable all auto-run options and source helpers ────────────

# prevent any pipeline stage from auto-executing when its runner script is
# loaded transitively
options(
  whep.run_pipeline.auto            = FALSE,
  whep.run_general_pipeline.auto    = FALSE,
  whep.run_import_pipeline.auto     = FALSE,
  whep.run_post_processing_pipeline.auto = FALSE,
  whep.run_export_pipeline.auto     = FALSE,
  whep.checkpointing.enabled        = FALSE
)

# resolve the project root regardless of working directory
.big_o_project_root <- tryCatch(
  here::here(),
  error = function(e) {
    # fallback: assume this script lives two levels below the project root
    # (scripts/complexity_analysis/big_o_estimation.R)
    normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
  }
)

.source_pipeline_script <- function(relative_path) {
  abs_path <- file.path(.big_o_project_root, "scripts", relative_path)
  if (!file.exists(abs_path)) {
    warning(sprintf("pipeline script not found, skipping: %s", abs_path))
    return(invisible(FALSE))
  }
  tryCatch(
    {
      source(abs_path, echo = FALSE, local = FALSE)
      invisible(TRUE)
    },
    error = function(e) {
      warning(sprintf("failed to source %s: %s", relative_path, conditionMessage(e)))
      invisible(FALSE)
    }
  )
}

# source the general pipeline helpers (pure functions, no side-effects)
.source_pipeline_script("0-general_pipeline/01-setup.R")
.source_pipeline_script("0-general_pipeline/02-helpers.R")

# source individual pipeline-stage function libraries (no I/O at top level)
.source_pipeline_script("1-import_pipeline/12-transform.R")
.source_pipeline_script("1-import_pipeline/13-validate_log.R")
.source_pipeline_script("1-import_pipeline/15-output.R")
.source_pipeline_script("2-post_processing_pipeline/24-standardize_units.R")
.source_pipeline_script("3-export_pipeline/31-export_lists.R")


# ── 1. analysis configuration ────────────────────────────────────────────────

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

# ── 2. synthetic data generators ────────────────────────────────────────────

#' @title sample product labels
.products     <- c("cereals", "oilseeds", "pulses", "fruits", "vegetables",
                   "sugar", "roots", "cotton", "tobacco", "fibres")
#' @title sample variable labels
.variables    <- c("production", "yield", "area_harvested", "import_quantity",
                   "export_quantity", "feed", "seed", "stock_variation")
#' @title sample unit labels
.units        <- c("tonnes", "kg_ha", "ha", "usd", "1000_usd", "head")
#' @title sample continent labels
.continents   <- c("asia", "europe", "africa", "americas", "oceania")
#' @title sample country pool
.countries    <- paste0("country_", formatC(1:80, width = 2, flag = "0"))

#' @title make synthetic wide-format data table
#' @description produces a data.table in the wide pipeline format with `n` rows
#'   and `n_years` year columns. safe for all import-stage benchmarks.
#' @param n integer. number of observation rows.
#' @param n_years integer. number of year columns (default 10).
#' @return data.table with key columns and year columns named "2000".."200x".
make_wide_dt <- function(n, n_years = 10L) {
  year_cols <- as.character(seq(2000L, 2000L + n_years - 1L))
  dt <- data.table::data.table(
    product    = sample(.products,   n, replace = TRUE),
    variable   = sample(.variables,  n, replace = TRUE),
    unit       = sample(.units,      n, replace = TRUE),
    continent  = sample(.continents, n, replace = TRUE),
    country    = sample(.countries,  n, replace = TRUE),
    footnotes  = sample(c(NA_character_, "e", "f", "p"), n,
                        replace = TRUE, prob = c(0.7, 0.1, 0.1, 0.1))
  )
  year_vals <- matrix(
    ifelse(
      stats::runif(n * n_years) < 0.9,
      as.character(round(stats::runif(n * n_years, 0, 1e6), 1)),
      NA_character_
    ),
    nrow = n,
    ncol = n_years
  )
  for (i in seq_along(year_cols)) {
    data.table::set(dt, j = year_cols[[i]], value = year_vals[, i])
  }
  return(dt)
}

#' @title make synthetic long-format data table
#' @description produces a data.table in the long pipeline format used by
#'   post-import stages. all columns match the expected schema.
#' @param n integer. number of observation rows.
#' @param na_fraction numeric in [0,1]. fraction of rows with NA value.
#' @param dup_fraction numeric in [0,1]. fraction of rows that are exact
#'   duplicates of another row (for duplicate-detection benchmarks).
#' @return data.table with full long-format schema.
make_long_dt <- function(n, na_fraction = 0.0, dup_fraction = 0.0) {
  n_dup   <- as.integer(floor(n * dup_fraction))
  n_orig  <- n - n_dup

  dt <- data.table::data.table(
    product   = sample(.products,   n_orig, replace = TRUE),
    variable  = sample(.variables,  n_orig, replace = TRUE),
    unit      = sample(.units,      n_orig, replace = TRUE),
    continent = sample(.continents, n_orig, replace = TRUE),
    country   = sample(.countries,  n_orig, replace = TRUE),
    year      = sample(as.character(1990L:2020L), n_orig, replace = TRUE),
    value     = ifelse(
      stats::runif(n_orig) < na_fraction,
      NA_character_,
      as.character(round(stats::runif(n_orig, 0, 1e6), 2))
    ),
    notes     = NA_character_,
    yearbook  = sample(c("yearbook_2020", "yearbook_2021"), n_orig, replace = TRUE),
    document  = sample(paste0("file_", formatC(1:5, width = 2, flag = "0"), ".xlsx"),
                       n_orig, replace = TRUE),
    footnotes = sample(c(NA_character_, "e", "f", "p"), n_orig,
                       replace = TRUE, prob = c(0.7, 0.1, 0.1, 0.1))
  )

  if (n_dup > 0L) {
    dup_idx <- sample(seq_len(n_orig), n_dup, replace = TRUE)
    dt      <- data.table::rbindlist(list(dt, dt[dup_idx]))
  }

  return(dt)
}

#' @title make synthetic numeric-string vector
#' @description creates a character vector suitable for `coerce_numeric_safe()`.
#' @param n integer. vector length.
#' @return character vector with mix of numeric strings, empty strings, and NAs.
make_numeric_string_vec <- function(n) {
  pool <- c(
    as.character(round(stats::runif(max(n, 100L), -1e6, 1e6), 4)),
    rep("", 5L),
    rep(NA_character_, 5L)
  )
  sample(pool, n, replace = TRUE)
}

#' @title make a minimal pipeline config for benchmarking
#' @description returns a minimal config list containing only the fields
#'   required by the benchmarked functions. does NOT create directories or
#'   perform any I/O.
#' @return named list (subset of full pipeline config).
make_benchmark_config <- function() {
  col_order <- c(
    "hemisphere", "continent", "country", "product", "variable",
    "unit", "year", "value", "notes", "yearbook", "document", "footnotes"
  )
  list(
    column_required = c("product", "variable", "unit", "continent", "country"),
    column_id       = c("product", "variable", "unit",
                        "continent", "country", "footnotes"),
    column_order    = col_order,
    defaults        = list(notes_value = NA_character_),
    columns = list(
      mandatory = c("product", "variable", "unit", "value"),
      base      = c("continent", "country", "unit", "footnotes"),
      id        = c("product", "variable", "unit",
                    "continent", "country", "footnotes"),
      value     = c("year", "value")
    )
  )
}


# ── 3. complexity model fitting ──────────────────────────────────────────────

#' @title complexity model candidates
#' @description returns a list of candidate complexity model descriptors.
#'   each entry has a `label` and a function `f(n)` that computes the
#'   predictor variable for a linear regression `t ~ f(n)`.
get_complexity_candidates <- function() {
  list(
    # O(1): predictor is all-zero so the lm() call falls through to the
    # intercept-only branch below.  slope is not meaningful for O(1).
    list(label = "O(1)",       f = function(n) rep(0, length(n))),
    list(label = "O(log n)",   f = function(n) log(n)),
    list(label = "O(n)",       f = function(n) n),
    list(label = "O(n log n)", f = function(n) n * log(n)),
    list(label = "O(n^2)",     f = function(n) n^2),
    list(label = "O(n^3)",     f = function(n) n^3)
  )
}

#' @title fit complexity model
#' @description fits each candidate complexity model via linear regression and
#'   returns the best-fitting class (by adjusted R²) together with all R²
#'   values, confidence intervals, and the fitted slope.
#' @param n_values numeric vector of input sizes.
#' @param t_values numeric vector of corresponding median elapsed times (seconds).
#' @return named list:
#'   \describe{
#'     \item{best_class}{character scalar — best-fitting complexity label}
#'     \item{best_r2}{numeric — adjusted R² of best model (NA if insufficient data)}
#'     \item{all_r2}{named numeric vector of adjusted R² per candidate}
#'     \item{slope_per_n}{numeric — fitted slope for best model per unit n}
#'   }
fit_complexity_model <- function(n_values, t_values) {
  valid <- !is.na(t_values) & !is.na(n_values) &
    is.finite(t_values) & is.finite(n_values) &
    t_values >= 0 & n_values > 0
  n <- as.numeric(n_values[valid])
  t <- as.numeric(t_values[valid])

  fallback <- list(
    best_class    = "unknown",
    best_r2       = NA_real_,
    all_r2        = setNames(rep(NA_real_, 6L),
                             vapply(get_complexity_candidates(),
                                    function(m) m$label, character(1))),
    slope_per_n   = NA_real_
  )

  if (length(n) < 3L) {
    return(fallback)
  }

  candidates  <- get_complexity_candidates()
  fit_results <- lapply(candidates, function(m) {
    x   <- m$f(n)
    # for O(1) the predictor is all-zero, use intercept-only model
    if (all(x == 0)) {
      fit <- tryCatch(stats::lm(t ~ 1), error = function(e) NULL)
    } else {
      df  <- data.frame(t = t, x = x)
      fit <- tryCatch(stats::lm(t ~ x, data = df), error = function(e) NULL)
    }
    if (is.null(fit)) return(list(label = m$label, r2 = -Inf, slope = NA_real_))
    summ <- summary(fit)
    r2   <- if (is.na(summ$adj.r.squared)) -Inf else summ$adj.r.squared
    coef <- stats::coef(fit)
    # slope is NA for O(1) (intercept-only) and for failed fits
    slope <- if (length(coef) >= 2L) unname(coef[2L]) else NA_real_
    list(label = m$label, r2 = r2, slope = slope)
  })

  r2_vec      <- vapply(fit_results, function(x) x$r2,    numeric(1))
  slope_vec   <- vapply(fit_results, function(x) x$slope, numeric(1))
  names(r2_vec)    <- vapply(candidates, function(m) m$label, character(1))
  names(slope_vec) <- names(r2_vec)

  best_idx    <- which.max(r2_vec)
  list(
    best_class  = fit_results[[best_idx]]$label,
    best_r2     = r2_vec[[best_idx]],
    all_r2      = r2_vec,
    slope_per_n = slope_vec[[best_idx]]
  )
}


# ── 4. timing harness ────────────────────────────────────────────────────────

#' @title time a function call
#' @description executes `fn()` once and returns elapsed wall-clock seconds.
#'   uses `proc.time()` which has sub-millisecond resolution on all platforms.
#' @param fn zero-argument function to time.
#' @return numeric scalar — elapsed seconds.
time_fn <- function(fn) {
  gc(verbose = FALSE, full = FALSE)
  t_start <- proc.time()[["elapsed"]]
  fn()
  t_end   <- proc.time()[["elapsed"]]
  t_end - t_start
}

#' @title run benchmark
#' @description times `fn` across all `input_sizes` with `n_reps` repetitions
#'   per size. generates fresh synthetic input before each repetition to avoid
#'   caching artefacts. returns a data.table of raw timing observations.
#' @param fn_factory function(n) that returns a zero-argument function to time.
#'   the inner function should capture all required inputs for one experiment.
#' @param input_sizes integer vector of input sizes.
#' @param n_reps integer number of replications per size.
#' @param quiet logical — suppress per-size progress messages.
#' @return data.table with columns: n (int), rep (int), elapsed_s (dbl).
run_benchmark <- function(fn_factory, input_sizes, n_reps = 5L, quiet = FALSE) {
  results <- vector("list", length(input_sizes))

  for (i in seq_along(input_sizes)) {
    n_i <- input_sizes[[i]]
    if (!quiet) {
      message(sprintf("  n = %d ...", n_i))
    }

    times <- numeric(n_reps)
    for (r in seq_len(n_reps)) {
      fn <- fn_factory(n_i)  # fresh input per rep
      times[[r]] <- time_fn(fn)
    }

    results[[i]] <- data.table::data.table(
      n         = rep(n_i,    n_reps),
      rep       = seq_len(n_reps),
      elapsed_s = times
    )
  }

  data.table::rbindlist(results)
}

#' @title summarise benchmark results
#' @description computes per-n statistics (median, mean, sd, min, max) from
#'   raw timing observations.
#' @param raw_dt data.table returned by `run_benchmark()`.
#' @return data.table with per-n summary statistics.
summarise_benchmark <- function(raw_dt) {
  raw_dt[,
    .(
      median_s = stats::median(elapsed_s),
      mean_s   = mean(elapsed_s),
      sd_s     = stats::sd(elapsed_s),
      min_s    = min(elapsed_s),
      max_s    = max(elapsed_s)
    ),
    by = n
  ][order(n)]
}


# ── 5. benchmark definitions ─────────────────────────────────────────────────
#
# each benchmark is described by a named list:
#   name       : display name for reporting
#   stage      : pipeline stage label
#   description: short description of what is measured
#   fn_factory : function(n) → function() that runs the target operation
#
# fn_factory must be self-contained (capture everything it needs via closure)
# and must NOT perform any I/O or modify global state.

#' @title build benchmark definitions
#' @description constructs all benchmark descriptors using the supplied config
#'   and rng_seed. the returned list is used by the execution engine.
#' @param cfg list returned by `get_big_o_config()`.
#' @return named list of benchmark descriptors.
build_benchmark_definitions <- function(cfg) {
  set.seed(cfg$rng_seed)
  bench_cfg  <- make_benchmark_config()
  n_yrs      <- cfg$n_year_cols
  na_frac    <- cfg$na_fraction
  dup_frac   <- cfg$dup_fraction

  benchmarks <- list(

    # ── Stage 0: General helpers ────────────────────────────────────────────

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
        # split n rows across 3 document tables
        chunk  <- max(1L, n %/% 3L)
        dt_list <- list(
          make_long_dt(chunk),
          make_long_dt(chunk),
          make_long_dt(n - 2L * chunk)
        )
        local_cfg <- bench_cfg
        function() {
          # consolidate_audited_dt expects copies (it modifies in place)
          input_list <- lapply(dt_list, data.table::copy)
          consolidate_audited_dt(input_list, local_cfg)
        }
      }
    ),

    # ── Stage 2: Post-processing pipeline ────────────────────────────────────

    list(
      name        = "aggregate_standardized_rows",
      stage       = "2-post_processing",
      description = paste0("group-sum n rows by all non-value columns (",
                            round(dup_frac * 100), "% duplicates to aggregate)"),
      fn_factory  = function(n) {
        dt_raw <- make_long_dt(n, dup_fraction = dup_frac)
        # convert value to numeric so aggregation is valid
        dt_raw[, value := suppressWarnings(as.numeric(value))]
        function() aggregate_standardized_rows(data.table::copy(dt_raw))
      }
    ),

    # ── Stage 3: Export pipeline ─────────────────────────────────────────────

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


# ── 6. execution engine ──────────────────────────────────────────────────────

#' @title run all benchmarks
#' @description iterates over all benchmark definitions, runs the timing
#'   harness for each, and returns raw timing data together with fitted
#'   complexity models.
#' @param benchmarks named list of benchmark descriptors (from
#'   `build_benchmark_definitions()`).
#' @param cfg analysis configuration from `get_big_o_config()`.
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table with all timing observations}
#'     \item{summary}{data.table with per-(function, n) statistics}
#'     \item{complexity}{data.table with one row per function, best-fit class,
#'       adjusted R², and dominant-stage flag}
#'   }
run_all_benchmarks <- function(benchmarks, cfg) {
  quiet       <- cfg$quiet
  input_sizes <- cfg$input_sizes
  n_reps      <- cfg$n_reps

  all_raw        <- vector("list", length(benchmarks))
  all_summary    <- vector("list", length(benchmarks))
  complexity_rows <- vector("list", length(benchmarks))

  for (i in seq_along(benchmarks)) {
    bm <- benchmarks[[i]]
    if (!quiet) {
      message(sprintf("\n[%d/%d] %s  (%s)",
                      i, length(benchmarks), bm$name, bm$stage))
      message(sprintf("  %s", bm$description))
    }

    raw_dt <- tryCatch(
      run_benchmark(bm$fn_factory, input_sizes, n_reps = n_reps, quiet = quiet),
      error = function(e) {
        warning(sprintf("benchmark '%s' failed: %s", bm$name, conditionMessage(e)))
        NULL
      }
    )

    if (is.null(raw_dt)) {
      # record failure row
      complexity_rows[[i]] <- data.table::data.table(
        fn_name     = bm$name,
        stage       = bm$stage,
        description = bm$description,
        best_class  = "ERROR",
        r_squared   = NA_real_,
        slope_per_n = NA_real_
      )
      next
    }

    raw_dt[, `:=`(fn_name = bm$name, stage = bm$stage)]
    all_raw[[i]] <- raw_dt

    summ_dt <- summarise_benchmark(raw_dt)
    summ_dt[, `:=`(fn_name = bm$name, stage = bm$stage)]
    all_summary[[i]] <- summ_dt

    fit <- fit_complexity_model(summ_dt$n, summ_dt$median_s)

    if (!quiet) {
      message(sprintf("  → best fit: %s  (adj. R² = %.4f)",
                      fit$best_class,
                      if (is.na(fit$best_r2)) NaN else fit$best_r2))
    }

    complexity_rows[[i]] <- data.table::data.table(
      fn_name     = bm$name,
      stage       = bm$stage,
      description = bm$description,
      best_class  = fit$best_class,
      r_squared   = fit$best_r2,
      slope_per_n = fit$slope_per_n
    )
  }

  raw_combined     <- data.table::rbindlist(all_raw,     fill = TRUE)
  summary_combined <- data.table::rbindlist(all_summary, fill = TRUE)
  complexity_dt    <- data.table::rbindlist(complexity_rows, fill = TRUE)

  # flag each function's overall pipeline complexity contribution
  # (worst-fitting class within each stage drives the stage complexity)
  complexity_order <- c("O(1)", "O(log n)", "O(n)", "O(n log n)",
                        "O(n^2)", "O(n^3)", "unknown", "ERROR")
  complexity_dt[,
    complexity_rank := match(best_class, complexity_order, nomatch = 7L)
  ]
  stage_max <- complexity_dt[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  complexity_dt <- stage_max[complexity_dt, on = "stage"]
  complexity_dt[,
    dominant_in_stage := (complexity_rank == stage_max_rank)
  ]
  complexity_dt[, stage_max_rank := NULL]

  list(
    raw        = raw_combined,
    summary    = summary_combined,
    complexity = complexity_dt
  )
}


# ── 7. reporting and visualisation ───────────────────────────────────────────

#' @title print complexity report
#' @description prints a formatted console table of complexity results with
#'   a summary of the dominant pipeline stage and overall complexity.
#' @param complexity_dt data.table from `run_all_benchmarks()$complexity`.
#' @return invisible(NULL)
print_complexity_report <- function(complexity_dt) {
  cat("\n")
  cat(strrep("─", 80L), "\n")
  cat("  WHEP PIPELINE — EMPIRICAL BIG O ANALYSIS\n")
  cat(strrep("─", 80L), "\n\n")

  # column widths
  w_fn    <- max(nchar(complexity_dt$fn_name),     12L) + 2L
  w_stage <- max(nchar(complexity_dt$stage),       10L) + 2L
  w_class <- max(nchar(complexity_dt$best_class),  10L) + 2L
  w_r2    <- 10L
  w_flag  <- 8L

  header <- sprintf(
    "%-*s %-*s %-*s %*s %*s",
    w_fn, "FUNCTION", w_stage, "STAGE",
    w_class, "COMPLEXITY", w_r2, "adj.R²", w_flag, "DOMINANT"
  )
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")

  for (i in seq_len(nrow(complexity_dt))) {
    row  <- complexity_dt[i]
    r2   <- if (is.na(row$r_squared)) "   N/A" else sprintf("%6.4f", row$r_squared)
    flag <- if (isTRUE(row$dominant_in_stage)) "  ★" else ""
    cat(sprintf(
      "%-*s %-*s %-*s %*s %*s\n",
      w_fn, row$fn_name, w_stage, row$stage,
      w_class, row$best_class, w_r2, r2, w_flag, flag
    ))
  }

  # per-stage summary
  cat("\n")
  cat(strrep("─", 80L), "\n")
  cat("  PER-STAGE DOMINANT COMPLEXITY\n")
  cat(strrep("─", 80L), "\n")

  complexity_order <- c("O(1)", "O(log n)", "O(n)", "O(n log n)",
                        "O(n^2)", "O(n^3)", "unknown", "ERROR")
  stage_summary <- complexity_dt[
    dominant_in_stage == TRUE,
    .(dominant_class = {
        # use first-alphabetically among tied dominants for determinism
        classes <- sort(unique(best_class))
        ranked  <- match(classes, complexity_order, nomatch = 7L)
        classes[[which.max(ranked)]]
      }),
    by = stage
  ][order(stage)]

  for (i in seq_len(nrow(stage_summary))) {
    cat(sprintf("  %-25s  %s\n",
                stage_summary$stage[[i]],
                stage_summary$dominant_class[[i]]))
  }

  # overall pipeline estimate (worst dominant stage class)
  all_ranks <- match(stage_summary$dominant_class, complexity_order, nomatch = 7L)
  overall   <- stage_summary$dominant_class[[which.max(all_ranks)]]
  cat("\n")
  cat(sprintf("  OVERALL PIPELINE ESTIMATE: %s\n", overall))
  cat(strrep("─", 80L), "\n\n")

  return(invisible(NULL))
}

#' @title export results as JSON
#' @description writes a structured JSON file summarising the complexity
#'   analysis results. uses a minimal pure-R JSON serialiser to avoid
#'   depending on `jsonlite`.
#' @param results list returned by `run_all_benchmarks()`.
#' @param output_path character scalar — path for the JSON file.
#' @return invisible character scalar — path where JSON was written.
export_results_json <- function(results, output_path) {
  complexity_dt <- results$complexity

  # build a plain-R list structure that maps cleanly to JSON
  per_fn <- lapply(seq_len(nrow(complexity_dt)), function(i) {
    row <- complexity_dt[i]
    list(
      fn_name          = row$fn_name,
      stage            = row$stage,
      description      = row$description,
      best_class       = row$best_class,
      adj_r_squared    = if (is.na(row$r_squared)) NULL else round(row$r_squared, 6L),
      slope_per_n      = if (is.na(row$slope_per_n)) NULL else row$slope_per_n,
      dominant_in_stage = isTRUE(row$dominant_in_stage)
    )
  })
  names(per_fn) <- complexity_dt$fn_name

  # per-stage summary
  stages <- unique(complexity_dt$stage)
  per_stage <- lapply(stages, function(s) {
    s_rows <- complexity_dt[stage == s]
    dom    <- s_rows[dominant_in_stage == TRUE]
    list(
      functions        = s_rows$fn_name,
      dominant_class   = if (nrow(dom) > 0L) dom$best_class[[1L]] else "unknown"
    )
  })
  names(per_stage) <- stages

  complexity_order <- c("O(1)", "O(log n)", "O(n)", "O(n log n)",
                        "O(n^2)", "O(n^3)", "unknown", "ERROR")
  dom_classes   <- vapply(per_stage, function(s) s$dominant_class, character(1))
  overall_rank  <- match(dom_classes, complexity_order, nomatch = 7L)
  overall_class <- dom_classes[[which.max(overall_rank)]]

  report <- list(
    analysis_timestamp     = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    overall_pipeline_class = overall_class,
    per_stage              = per_stage,
    per_function           = per_fn
  )

  # minimal R-to-JSON serialiser (no external deps)
  to_json <- function(x, indent = 0L) {
    pad  <- strrep("  ", indent)
    pad2 <- strrep("  ", indent + 1L)

    if (is.null(x)) return("null")
    if (is.logical(x) && length(x) == 1L) return(if (x) "true" else "false")
    if (is.numeric(x) && length(x) == 1L) {
      if (is.nan(x) || is.infinite(x)) return("null")
      return(sprintf("%.15g", x))
    }
    if (is.character(x) && length(x) == 1L) {
      escaped <- gsub("\\", "\\\\", x,       fixed = TRUE)  # backslashes first
      escaped <- gsub('"',  '\\"',  escaped,  fixed = TRUE)  # then double-quotes
      escaped <- gsub("\n", "\\n",  escaped,  fixed = TRUE)  # newlines
      escaped <- gsub("\r", "\\r",  escaped,  fixed = TRUE)  # carriage returns
      escaped <- gsub("\t", "\\t",  escaped,  fixed = TRUE)  # tabs
      escaped <- gsub("\f", "\\f",  escaped,  fixed = TRUE)  # form-feed
      escaped <- gsub("\b", "\\b",  escaped,  fixed = TRUE)  # backspace
      # escape remaining C0 control characters (\x01-\x1f, excluding those
      # handled explicitly above).  NUL (\x00, cp = 0) is intentionally
      # excluded from the range: rawToChar(as.raw(0)) returns "" (R stops at
      # the NUL terminator), and passing an empty-string pattern to gsub()
      # raises a "zero-length pattern" error.  NUL bytes cannot appear in
      # normal R character strings, so skipping cp = 0 is safe.
      for (cp in setdiff(1L:31L, c(8L, 9L, 10L, 12L, 13L))) {
        chr <- rawToChar(as.raw(cp))
        if (!nzchar(chr)) next  # safety guard: never pass "" to gsub()
        escaped <- gsub(chr,
                        sprintf("\\u%04x", cp),
                        escaped, fixed = TRUE)
      }
      return(paste0('"', escaped, '"'))
    }
    if (is.character(x) || is.numeric(x) || is.logical(x)) {
      elems <- vapply(x, function(v) to_json(v, indent + 1L), character(1))
      return(paste0("[\n",
                    paste0(pad2, elems, collapse = ",\n"),
                    "\n", pad, "]"))
    }
    if (is.list(x)) {
      nms <- names(x)
      # treat as a JSON object when every element has a non-empty name; fall
      # back to a JSON array otherwise (handles NULL names, zero-length x, or
      # partially named lists where some names are empty strings).
      if (!is.null(nms) && length(nms) == length(x) && all(nzchar(nms))) {
        pairs <- mapply(function(k, v) {
          paste0(pad2, '"', k, '": ', to_json(v, indent + 1L))
        }, nms, x, SIMPLIFY = TRUE, USE.NAMES = FALSE)
        return(paste0("{\n",
                      paste(pairs, collapse = ",\n"),
                      "\n", pad, "}"))
      } else {
        elems <- lapply(x, function(v) paste0(pad2, to_json(v, indent + 1L)))
        return(paste0("[\n",
                      paste(elems, collapse = ",\n"),
                      "\n", pad, "]"))
      }
    }
    return(paste0('"', as.character(x), '"'))
  }

  json_str <- to_json(report)

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(json_str, con = output_path)
  message(sprintf("  JSON summary written to: %s", output_path))
  return(invisible(output_path))
}

#' @title produce runtime plots
#' @description saves one plot per benchmark function showing median elapsed
#'   time vs. input size, with the fitted complexity class in the title.
#'   uses `ggplot2` when available, otherwise falls back to base R graphics.
#' @param results list returned by `run_all_benchmarks()`.
#' @param plots_dir character scalar — directory for PNG output files.
#' @return invisible character vector — paths of produced plot files.
write_complexity_plots <- function(results, plots_dir) {
  dir.create(plots_dir, recursive = TRUE, showWarnings = FALSE)
  summary_dt    <- results$summary
  complexity_dt <- results$complexity

  fn_names  <- unique(summary_dt$fn_name)
  use_ggplot <- requireNamespace("ggplot2", quietly = TRUE)

  produced_files <- character(0L)

  for (fn in fn_names) {
    s_dt  <- summary_dt[fn_name == fn][order(n)]
    c_row    <- complexity_dt[fn_name == fn]
    r2_val   <- if (nrow(c_row) > 0L) c_row$r_squared[[1L]] else NA_real_
    cls_val  <- if (nrow(c_row) > 0L) c_row$best_class[[1L]] else "?"
    title    <- sprintf("%s\n%s  (adj.R²=%s)",
                        fn, cls_val,
                        if (is.na(r2_val)) "NA" else sprintf("%.3f", r2_val))
    out_path <- file.path(plots_dir, paste0(fn, ".png"))

    tryCatch({
      grDevices::png(out_path, width = 800L, height = 500L, res = 100L)

      if (use_ggplot) {
        # ggplot2 path
        p <- ggplot2::ggplot(
          s_dt,
          ggplot2::aes(x = n, y = median_s * 1000)
        ) +
          ggplot2::geom_ribbon(
            ggplot2::aes(ymin = min_s * 1000, ymax = max_s * 1000),
            alpha = 0.15, fill = "#2166AC"
          ) +
          ggplot2::geom_line(colour = "#2166AC", linewidth = 0.9) +
          ggplot2::geom_point(colour = "#D73027", size = 2.5) +
          ggplot2::labs(
            title    = title,
            x        = "Input size  n",
            y        = "Median elapsed time  (ms)",
            caption  = sprintf("stage: %s  |  reps per n: %d",
                                if (nrow(c_row) > 0L) c_row$stage[[1L]] else "?",
                                nrow(results$raw[fn_name == fn]) /
                                  length(unique(s_dt$n)))
          ) +
          ggplot2::theme_bw(base_size = 12L) +
          ggplot2::theme(plot.title = ggplot2::element_text(size = 11L))
        print(p)
      } else {
        # base-R fallback
        # draw empty frame first, then polygon, then lines/points
        graphics::plot(
          x    = s_dt$n,
          y    = s_dt$median_s * 1000,
          type = "n",
          main = title,
          xlab = "Input size  n",
          ylab = "Median elapsed time  (ms)",
          las  = 1L
        )
        # shaded min-max band (drawn before lines so it sits behind them)
        graphics::polygon(
          x      = c(s_dt$n, rev(s_dt$n)),
          y      = c(s_dt$min_s, rev(s_dt$max_s)) * 1000,
          col    = grDevices::adjustcolor("#2166AC", alpha.f = 0.15),
          border = NA
        )
        graphics::lines(s_dt$n, s_dt$median_s * 1000, col = "#2166AC", lwd = 1.5)
        graphics::points(s_dt$n, s_dt$median_s * 1000, col = "#D73027",
                         pch = 19L, cex = 1.2)
      }

      grDevices::dev.off()
      produced_files <- c(produced_files, out_path)

    }, error = function(e) {
      # ensure device is closed even on error
      tryCatch(grDevices::dev.off(), error = function(e2) NULL)
      warning(sprintf("plot failed for '%s': %s", fn, conditionMessage(e)))
    })
  }

  message(sprintf("  %d plot(s) written to: %s", length(produced_files), plots_dir))
  return(invisible(produced_files))
}


# ── 8. main entry point ──────────────────────────────────────────────────────

#' @title run Big O analysis
#' @description top-level entry point. runs the full empirical Big O analysis
#'   for the WHEP pipeline and produces console output, a JSON summary, and
#'   runtime plots.
#'
#' @param cfg list. analysis configuration. defaults to `get_big_o_config()`.
#'   override individual fields to change input sizes, reps, output location,
#'   etc.
#' @param output_dir character scalar. directory for JSON and plots. overrides
#'   `cfg$output_dir` when supplied. defaults to `tempdir()/big_o_analysis`.
#' @param write_plots logical. whether to write PNG plots. overrides
#'   `cfg$produce_plots` when supplied.
#' @param quiet logical. suppress progress messages.
#'
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table — all timing observations}
#'     \item{summary}{data.table — per-(function, n) statistics}
#'     \item{complexity}{data.table — complexity class per function}
#'     \item{json_path}{character — path of written JSON file}
#'     \item{plot_paths}{character — paths of written PNG files}
#'   }
#'
#' @examples
#' \dontrun{
#' # run with defaults (outputs to tempdir)
#' results <- run_big_o_analysis()
#'
#' # fast smoke-test with tiny inputs
#' results <- run_big_o_analysis(
#'   cfg = utils::modifyList(get_big_o_config(), list(
#'     input_sizes = c(100L, 500L, 1000L),
#'     n_reps      = 3L
#'   ))
#' )
#'
#' # custom output directory
#' results <- run_big_o_analysis(output_dir = "~/analysis/big_o")
#' }
run_big_o_analysis <- function(
  cfg        = get_big_o_config(),
  output_dir = NULL,
  write_plots = NULL,
  quiet       = FALSE
) {
  if (!is.null(output_dir)) cfg$output_dir    <- output_dir
  if (!is.null(write_plots)) cfg$produce_plots <- write_plots
  cfg$quiet <- quiet

  if (is.null(cfg$output_dir)) {
    cfg$output_dir <- file.path(tempdir(), "big_o_analysis")
  }

  start_time <- proc.time()[["elapsed"]]
  if (!quiet) {
    message(strrep("─", 70L))
    message("  WHEP Pipeline — Empirical Big O Analysis")
    message(sprintf("  input_sizes : %s", paste(cfg$input_sizes, collapse = ", ")))
    message(sprintf("  n_reps      : %d", cfg$n_reps))
    message(sprintf("  output_dir  : %s", cfg$output_dir))
    message(strrep("─", 70L))
  }

  benchmarks <- build_benchmark_definitions(cfg)
  results    <- run_all_benchmarks(benchmarks, cfg)

  print_complexity_report(results$complexity)

  json_path  <- file.path(cfg$output_dir, "big_o_summary.json")
  export_results_json(results, json_path)

  plot_paths <- character(0L)
  if (isTRUE(cfg$produce_plots)) {
    plots_dir  <- file.path(cfg$output_dir, "plots")
    plot_paths <- write_complexity_plots(results, plots_dir)
  }

  elapsed <- proc.time()[["elapsed"]] - start_time
  if (!quiet) {
    message(sprintf("\n  Analysis complete in %.1f s", elapsed))
    message(strrep("─", 70L))
  }

  results$json_path  <- json_path
  results$plot_paths <- plot_paths
  return(invisible(results))
}


# ── auto-run guard ────────────────────────────────────────────────────────────
# run_big_o_analysis() is NOT called automatically when this script is sourced.
# invoke it explicitly:
#
#   results <- run_big_o_analysis()
#
# or with a custom config:
#
#   results <- run_big_o_analysis(
#     cfg = utils::modifyList(get_big_o_config(), list(
#       input_sizes = c(500L, 2000L, 10000L),
#       n_reps      = 10L,
#       output_dir  = "~/big_o_out"
#     ))
#   )
