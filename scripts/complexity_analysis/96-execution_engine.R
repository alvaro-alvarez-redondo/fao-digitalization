# script: 96-execution_engine
# description: benchmark execution engine for the Big O complexity analysis
#   module. iterates over all benchmark definitions, times each one, fits the
#   complexity model, and aggregates the results into a single structured list.
#
# depends on: 93-complexity_models.R  (fit_complexity_model)
#             94-timing_harness.R     (run_benchmark, summarise_benchmark)
# sourced by:  run_complexity_analysis.R

# ── 6. execution engine ──────────────────────────────────────────────────────

#' @title run all benchmarks
#' @description iterates over all benchmark definitions, runs the timing
#'   harness for each, and returns raw timing data together with fitted
#'   complexity models. when `cfg$quiet` is FALSE each benchmark run is wrapped
#'   in its own `progressr::with_progress()` call so the console shows a
#'   dedicated 1/T … T/T progress bar (T = number of input sizes) per function.
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
#' @importFrom progressr with_progress progressor
run_all_benchmarks <- function(benchmarks, cfg) {
  quiet <- cfg$quiet
  input_sizes <- cfg$input_sizes
  n_reps <- cfg$n_reps
  n_sizes <- length(input_sizes)

  all_raw <- vector("list", length(benchmarks))
  all_summary <- vector("list", length(benchmarks))
  complexity_rows <- vector("list", length(benchmarks))

  for (i in seq_along(benchmarks)) {
    bm <- benchmarks[[i]]
    if (!quiet) {
      message(sprintf(
        "\n[%d/%d] %s  (%s)",
        i,
        length(benchmarks),
        bm$name,
        bm$stage
      ))
      message(sprintf("  %s", bm$description))
    }

    raw_dt <- tryCatch(
      {
        if (!quiet) {
          progressr::with_progress({
            p <- progressr::progressor(steps = n_sizes)
            run_benchmark(
              bm$fn_factory,
              input_sizes,
              n_reps = n_reps,
              quiet = TRUE,
              progressor = p
            )
          })
        } else {
          run_benchmark(
            bm$fn_factory,
            input_sizes,
            n_reps = n_reps,
            quiet = TRUE,
            progressor = NULL
          )
        }
      },
      error = function(e) {
        warning(sprintf(
          "benchmark '%s' failed: %s",
          bm$name,
          conditionMessage(e)
        ))
        NULL
      }
    )

    if (is.null(raw_dt)) {
      # record failure row
      complexity_rows[[i]] <- data.table::data.table(
        fn_name = bm$name,
        stage = bm$stage,
        description = bm$description,
        best_class = "ERROR",
        r_squared = NA_real_,
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
      message(sprintf(
        "  → best fit: %s  (adj. R² = %.4f)",
        fit$best_class,
        if (is.na(fit$best_r2)) NaN else fit$best_r2
      ))
    }

    complexity_rows[[i]] <- data.table::data.table(
      fn_name = bm$name,
      stage = bm$stage,
      description = bm$description,
      best_class = fit$best_class,
      r_squared = fit$best_r2,
      slope_per_n = fit$slope_per_n
    )
  }

  raw_combined <- data.table::rbindlist(all_raw, fill = TRUE)
  summary_combined <- data.table::rbindlist(all_summary, fill = TRUE)
  complexity_dt <- data.table::rbindlist(complexity_rows, fill = TRUE)

  # flag each function's overall pipeline complexity contribution
  # (worst-fitting class within each stage drives the stage complexity)
  complexity_order <- c(
    "O(1)",
    "O(log n)",
    "O(n)",
    "O(n log n)",
    "O(n^2)",
    "O(n^3)",
    "unknown",
    "ERROR"
  )
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
    raw = raw_combined,
    summary = summary_combined,
    complexity = complexity_dt
  )
}
