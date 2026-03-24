# complexity_analysis/execution_engine.R
# description: benchmark execution engine for the Big O complexity analysis
#   module. iterates over all benchmark definitions, times each one, fits the
#   complexity model, and delegates aggregation to the aggregation layer.
#
# depends on: timing_engine.R     (run_benchmark, summarise_benchmark)
#             model_fitting.R     (fit_complexity_model)
#             aggregation.R       (add_stage_dominance)
# sourced by:  complexity_analysis/run_analysis.R

# ── execution engine ─────────────────────────────────────────────────────────

#' @title run all benchmarks
#' @description iterates over all benchmark definitions, runs the timing
#'   harness for each, fits the complexity model, and returns structured
#'   results. when `cfg$quiet` is FALSE each benchmark run is wrapped in
#'   its own `progressr::with_progress()` call.
#' @param benchmarks named list of benchmark descriptors (from
#'   `build_benchmark_definitions()`).
#' @param cfg analysis configuration from `get_big_o_config()`.
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table with all timing observations}
#'     \item{summary}{data.table with per-(function, n) statistics}
#'     \item{complexity}{data.table with one row per function, best-fit class,
#'       adjusted R², stage, and dominant-stage flag}
#'   }
run_all_benchmarks <- function(benchmarks, cfg) {
  quiet       <- cfg$quiet
  input_sizes <- cfg$input_sizes
  n_reps      <- cfg$n_reps
  n_sizes     <- length(input_sizes)

  all_raw      <- vector("list", length(benchmarks))
  all_summary  <- vector("list", length(benchmarks))
  cmplx_rows   <- vector("list", length(benchmarks))

  for (i in seq_along(benchmarks)) {
    bm <- benchmarks[[i]]
    if (!quiet) {
      message(sprintf(
        "\n[%d/%d] %s  (%s)",
        i, length(benchmarks), bm$name, bm$stage
      ))
      message(sprintf("  %s", bm$description))
    }

    raw_dt <- tryCatch(
      {
        if (!quiet) {
          progressr::with_progress({
            p <- progressr::progressor(steps = n_sizes)
            run_benchmark(bm$fn_factory, input_sizes,
                          n_reps = n_reps, quiet = TRUE, progressor = p)
          })
        } else {
          run_benchmark(bm$fn_factory, input_sizes,
                        n_reps = n_reps, quiet = TRUE, progressor = NULL)
        }
      },
      error = function(e) {
        warning(sprintf("benchmark '%s' failed: %s", bm$name, conditionMessage(e)))
        NULL
      }
    )

    if (is.null(raw_dt)) {
      cmplx_rows[[i]] <- data.table::data.table(
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
      message(sprintf(
        "  → best fit: %s  (adj. R² = %.4f)",
        fit$best_class,
        if (is.na(fit$best_r2)) NaN else fit$best_r2
      ))
    }

    cmplx_rows[[i]] <- data.table::data.table(
      fn_name     = bm$name,
      stage       = bm$stage,
      description = bm$description,
      best_class  = fit$best_class,
      r_squared   = fit$best_r2,
      slope_per_n = fit$slope_per_n
    )
  }

  raw_combined     <- data.table::rbindlist(all_raw, fill = TRUE)
  summary_combined <- data.table::rbindlist(all_summary, fill = TRUE)
  complexity_dt    <- data.table::rbindlist(cmplx_rows, fill = TRUE)

  # delegate stage-dominance annotation to the aggregation layer
  complexity_dt <- add_stage_dominance(complexity_dt)

  list(
    raw        = raw_combined,
    summary    = summary_combined,
    complexity = complexity_dt
  )
}
