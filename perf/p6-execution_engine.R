# module:      p6-execution_engine
# description: execution orchestration for the perf module.
#   provides per-stage analysis (run_stage_analysis), global aggregation
#   (run_all_stages), and the backward-compatible flat runner
#   (run_all_benchmarks) that accepts a pre-built benchmark catalogue.
#
# depends on:  p3-complexity_models.R  (fit_complexity_model, .complexity_order)
#              p4-timing_harness.R     (run_benchmark, summarise_benchmark)
#              p5-workload_generators.R (build_stage_benchmarks,
#                                        build_benchmark_definitions)
# sourced by:  perf/run_perf.R

# ── 6. execution engine ──────────────────────────────────────────────────────

#' @title run benchmark set and fit complexity models
#' @description internal helper: runs `run_benchmark` for every benchmark in
#'   `benchmarks`, computes summary statistics, fits a complexity model, and
#'   assembles a structured result list. used by both `run_stage_analysis` and
#'   `run_all_benchmarks`.
#' @param benchmarks named list of benchmark descriptors.
#' @param cfg named list from `get_analysis_config()`.
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table with all timing observations}
#'     \item{summary}{data.table with per-(function, n) statistics}
#'     \item{complexity}{data.table with one row per function: fn_name, stage,
#'       description, best_class, r_squared, slope_per_n, complexity_rank,
#'       dominant_in_stage}
#'   }
.run_benchmark_set <- function(benchmarks, cfg) {
  quiet       <- cfg$quiet
  input_sizes <- cfg$input_sizes
  n_reps      <- cfg$n_reps
  n_sizes     <- length(input_sizes)

  all_raw        <- vector("list", length(benchmarks))
  all_summary    <- vector("list", length(benchmarks))
  complexity_rows <- vector("list", length(benchmarks))

  for (i in seq_along(benchmarks)) {
    bm <- benchmarks[[i]]
    if (!quiet) {
      message(sprintf(
        "\n[%d/%d] %s  (%s)", i, length(benchmarks), bm$name, bm$stage
      ))
      message(sprintf("  %s", bm$description))
    }

    raw_dt <- tryCatch(
      {
        if (!quiet) {
          progressr::with_progress({
            p <- progressr::progressor(steps = n_sizes)
            run_benchmark(bm$fn_factory, input_sizes, n_reps = n_reps,
                          quiet = TRUE, progressor = p)
          })
        } else {
          run_benchmark(bm$fn_factory, input_sizes, n_reps = n_reps,
                        quiet = TRUE, progressor = NULL)
        }
      },
      error = function(e) {
        warning(sprintf("benchmark '%s' failed: %s", bm$name, conditionMessage(e)))
        NULL
      }
    )

    if (is.null(raw_dt)) {
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
      message(sprintf(
        "  \u2192 best fit: %s  (adj. R\u00b2 = %.4f)",
        fit$best_class,
        if (is.na(fit$best_r2)) NaN else fit$best_r2
      ))
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

  raw_combined     <- data.table::rbindlist(all_raw,        fill = TRUE)
  summary_combined <- data.table::rbindlist(all_summary,    fill = TRUE)
  complexity_dt    <- data.table::rbindlist(complexity_rows, fill = TRUE)

  complexity_dt[,
    complexity_rank := match(best_class, names(.complexity_order),
                             nomatch = .complexity_order[["unknown"]])
  ]
  stage_max <- complexity_dt[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  complexity_dt <- stage_max[complexity_dt, on = "stage"]
  complexity_dt[, dominant_in_stage := (complexity_rank == stage_max_rank)]
  complexity_dt[, stage_max_rank := NULL]

  list(raw = raw_combined, summary = summary_combined, complexity = complexity_dt)
}

#' @title run analysis for a single pipeline stage
#' @description benchmarks all functions defined for `stage_id`, fits complexity
#'   models, and returns a structured result list. the result is identical in
#'   shape to the output of `run_all_benchmarks()` but scoped to one stage.
#' @param stage_id character scalar — one of "0-general", "1-import",
#'   "2-post_processing", "3-export".
#' @param cfg named list from `get_analysis_config()`.
#' @return named list with elements `raw`, `summary`, `complexity` (see
#'   `.run_benchmark_set()` for schema details).
run_stage_analysis <- function(stage_id, cfg) {
  if (!cfg$quiet) {
    message(strrep("\u2500", 60L))
    message(sprintf("  Stage: %s", stage_id))
    message(strrep("\u2500", 60L))
  }
  benchmarks <- build_stage_benchmarks(stage_id, cfg)
  .run_benchmark_set(benchmarks, cfg)
}

#' @title run analysis for all pipeline stages
#' @description runs `run_stage_analysis()` for each stage listed in
#'   `cfg$stages` and returns a per-stage named list together with a global
#'   aggregation across all stages.
#' @param cfg named list from `get_analysis_config()`.
#' @return named list:
#'   \describe{
#'     \item{by_stage}{named list — one result list per stage (same shape as
#'       `run_stage_analysis()` output)}
#'     \item{raw}{data.table — all timing observations across all stages}
#'     \item{summary}{data.table — per-(function, n) statistics, all stages}
#'     \item{complexity}{data.table — per-function complexity, all stages}
#'   }
run_all_stages <- function(cfg) {
  set.seed(cfg$rng_seed)

  by_stage <- lapply(cfg$stages, function(s) run_stage_analysis(s, cfg))
  names(by_stage) <- cfg$stages

  global_raw     <- data.table::rbindlist(
    lapply(by_stage, function(r) r$raw),     fill = TRUE)
  global_summary <- data.table::rbindlist(
    lapply(by_stage, function(r) r$summary), fill = TRUE)
  global_cmplx   <- data.table::rbindlist(
    lapply(by_stage, function(r) r$complexity), fill = TRUE)

  # recompute dominant_in_stage on the combined table
  global_cmplx[,
    complexity_rank := match(best_class, names(.complexity_order),
                             nomatch = .complexity_order[["unknown"]])
  ]
  stage_max <- global_cmplx[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  global_cmplx <- stage_max[global_cmplx, on = "stage"]
  global_cmplx[, dominant_in_stage := (complexity_rank == stage_max_rank)]
  global_cmplx[, stage_max_rank := NULL]

  list(
    by_stage   = by_stage,
    raw        = global_raw,
    summary    = global_summary,
    complexity = global_cmplx
  )
}

#' @title run all benchmarks (flat mode)
#' @description backward-compatible runner that accepts a pre-built flat
#'   benchmark catalogue (e.g. from `build_benchmark_definitions()`) and runs
#'   all entries. callers that already hold a benchmark list can use this
#'   function directly without restructuring.
#' @param benchmarks named list of benchmark descriptors.
#' @param cfg named list from `get_analysis_config()`.
#' @return named list with elements `raw`, `summary`, `complexity`.
run_all_benchmarks <- function(benchmarks, cfg) {
  .run_benchmark_set(benchmarks, cfg)
}
