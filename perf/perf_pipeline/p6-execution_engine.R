#' @title Execution engine module
#' @description Stage-level and aggregate benchmark execution utilities for the
#'   performance analysis framework.
#' @keywords internal
#' @noRd
NULL

# ── 6. execution engine ──────────────────────────────────────────────────────

#' @title Check complexity fit support
#' @description Validate whether a candidate complexity input vector provides
#'   enough distinct support to fit models reliably under configured thresholds.
#' @param n_values Numeric vector of candidate input sizes.
#' @param min_unique_n_req Integer scalar minimum number of distinct values.
#' @param min_n_span_ratio_req Numeric scalar minimum max(n) / min(n) ratio.
#' @return Logical scalar.
#' @keywords internal
#' @noRd
.has_complexity_fit_support <- function(
  n_values,
  min_unique_n_req,
  min_n_span_ratio_req
) {
  n_clean <- suppressWarnings(as.numeric(n_values))
  valid <- !is.na(n_clean) & is.finite(n_clean) & n_clean > 0
  n_clean <- n_clean[valid]

  if (length(n_clean) < 3L) {
    return(FALSE)
  }

  unique_n <- sort(unique(n_clean))
  if (length(unique_n) < as.integer(min_unique_n_req)) {
    return(FALSE)
  }

  n_span_ratio <- max(unique_n) / min(unique_n)
  if (!is.finite(n_span_ratio) || n_span_ratio < as.numeric(min_n_span_ratio_req)) {
    return(FALSE)
  }

  return(TRUE)
}

#' @title Select complexity fit input sizes
#' @description Choose between transformed and original benchmark n values.
#'   Uses transformed values when they satisfy minimum fit-support thresholds;
#'   otherwise falls back to original values when those satisfy support.
#' @param original_n Numeric vector of original benchmark sizes.
#' @param transformed_n Optional numeric vector after benchmark-specific
#'   complexity transform.
#' @param min_unique_n_req Integer scalar minimum number of distinct values.
#' @param min_n_span_ratio_req Numeric scalar minimum max(n) / min(n) ratio.
#' @return Named list with n_values, source, and fallback_applied.
#' @keywords internal
#' @noRd
.select_complexity_fit_n <- function(
  original_n,
  transformed_n = NULL,
  min_unique_n_req,
  min_n_span_ratio_req
) {
  original_n <- suppressWarnings(as.numeric(original_n))

  if (is.null(transformed_n)) {
    return(list(
      n_values = original_n,
      source = "original",
      fallback_applied = FALSE
    ))
  }

  transformed_n <- suppressWarnings(as.numeric(transformed_n))

  transformed_has_support <- .has_complexity_fit_support(
    transformed_n,
    min_unique_n_req = min_unique_n_req,
    min_n_span_ratio_req = min_n_span_ratio_req
  )

  if (isTRUE(transformed_has_support)) {
    return(list(
      n_values = transformed_n,
      source = "transformed",
      fallback_applied = FALSE
    ))
  }

  original_has_support <- .has_complexity_fit_support(
    original_n,
    min_unique_n_req = min_unique_n_req,
    min_n_span_ratio_req = min_n_span_ratio_req
  )

  if (isTRUE(original_has_support)) {
    return(list(
      n_values = original_n,
      source = "original",
      fallback_applied = TRUE
    ))
  }

  return(list(
    n_values = transformed_n,
    source = "transformed",
    fallback_applied = FALSE
  ))
}

#' @title Run benchmark set
#' @description Internal helper that executes benchmark descriptors and fits
#'   complexity models.
#' @param benchmarks A named list of benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A named list:
#'   \describe{
#'     \item{raw}{data.table with all timing observations}
#'     \item{summary}{data.table with per-(function, n) statistics}
#'     \item{complexity}{data.table with one row per function: fn_name, stage,
#'       description, best_class, r_squared, slope_per_n, complexity_rank,
#'       dominant_in_stage}
#'   }
#' @keywords internal
#' @noRd
.run_benchmark_set <- function(benchmarks, cfg) {
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

    bm_r2_tolerance <- if (!is.null(bm$complexity_r2_tolerance)) {
      bm$complexity_r2_tolerance
    } else {
      cfg$complexity_r2_tolerance
    }
    bm_min_best_r2 <- if (!is.null(bm$complexity_min_best_r2)) {
      bm$complexity_min_best_r2
    } else {
      cfg$complexity_min_best_r2
    }
    bm_min_unique_n <- if (!is.null(bm$complexity_min_unique_n)) {
      bm$complexity_min_unique_n
    } else {
      cfg$complexity_min_unique_n
    }
    bm_min_n_span_ratio <- if (!is.null(bm$complexity_min_n_span_ratio)) {
      bm$complexity_min_n_span_ratio
    } else {
      cfg$complexity_min_n_span_ratio
    }

    fit_min_unique_n <- .resolve_complexity_min_unique_n(bm_min_unique_n)
    fit_min_n_span_ratio <- .resolve_complexity_min_n_span_ratio(bm_min_n_span_ratio)

    fit_input_n <- as.numeric(summ_dt$n)
    if (!is.null(bm$complexity_n_transform) && is.function(bm$complexity_n_transform)) {
      transformed_n <- tryCatch(
        bm$complexity_n_transform(summ_dt$n),
        error = function(e) {
          warning(sprintf(
            "benchmark '%s' complexity_n_transform failed: %s",
            bm$name,
            conditionMessage(e)
          ))
          NULL
        }
      )

      if (!is.null(transformed_n)) {
        transformed_n <- suppressWarnings(as.numeric(transformed_n))
        valid_transform <- (
          length(transformed_n) == length(fit_input_n) &&
            all(!is.na(transformed_n)) &&
            all(is.finite(transformed_n)) &&
            all(transformed_n > 0)
        )
        if (isTRUE(valid_transform)) {
          selected_n <- .select_complexity_fit_n(
            original_n = summ_dt$n,
            transformed_n = transformed_n,
            min_unique_n_req = fit_min_unique_n,
            min_n_span_ratio_req = fit_min_n_span_ratio
          )
          fit_input_n <- selected_n$n_values

          if (!quiet && isTRUE(selected_n$fallback_applied)) {
            message(sprintf(
              "  -> complexity transform collapsed support for '%s'; using original n for model fitting",
              bm$name
            ))
          }
        } else {
          warning(sprintf(
            "benchmark '%s' returned invalid transformed complexity n values; using original n",
            bm$name
          ))
        }
      }
    }

    fit_dt <- data.table::data.table(
      n = fit_input_n,
      median_s = summ_dt$median_s
    )
    fit_dt <- fit_dt[
      ,
      .(median_s = stats::median(median_s, na.rm = TRUE)),
      by = n
    ][order(n)]

    fit <- fit_complexity_model(
      fit_dt$n,
      fit_dt$median_s,
      r2_tolerance = bm_r2_tolerance,
      min_best_r2 = bm_min_best_r2,
      min_unique_n = bm_min_unique_n,
      min_n_span_ratio = bm_min_n_span_ratio
    )

    if (!quiet) {
      message(sprintf(
        "  \u2192 best fit: %s  (adj. R\u00b2 = %.4f)",
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

  complexity_dt[,
    complexity_rank := match(
      best_class,
      names(.complexity_order),
      nomatch = .complexity_order[["unknown"]]
    )
  ]
  stage_max <- complexity_dt[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  complexity_dt <- stage_max[complexity_dt, on = "stage"]
  complexity_dt[, dominant_in_stage := (complexity_rank == stage_max_rank)]
  complexity_dt[, stage_max_rank := NULL]

  list(
    raw = raw_combined,
    summary = summary_combined,
    complexity = complexity_dt
  )
}

#' @title Run stage analysis
#' @description Run benchmarks and model fitting for one pipeline stage.
#' @param stage_id A character scalar stage identifier.
#' @param cfg A named list from get_analysis_config().
#' @return A named list with elements raw, summary, and complexity.
run_stage_analysis <- function(stage_id, cfg) {
  if (!cfg$quiet) {
    message(strrep("\u2500", 60L))
    message(sprintf("  Stage: %s", stage_id))
    message(strrep("\u2500", 60L))
  }
  benchmarks <- build_stage_benchmarks(stage_id, cfg)
  .run_benchmark_set(benchmarks, cfg)
}

#' @title Run analysis for all stages
#' @description Execute run_stage_analysis() across all configured stages and
#'   return stage-level plus aggregate outputs.
#' @param cfg A named list from get_analysis_config().
#' @return A named list:
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

  global_raw <- data.table::rbindlist(
    lapply(by_stage, function(r) r$raw),
    fill = TRUE
  )
  global_summary <- data.table::rbindlist(
    lapply(by_stage, function(r) r$summary),
    fill = TRUE
  )
  global_cmplx <- data.table::rbindlist(
    lapply(by_stage, function(r) r$complexity),
    fill = TRUE
  )

  # recompute dominant_in_stage on the combined table
  global_cmplx[,
    complexity_rank := match(
      best_class,
      names(.complexity_order),
      nomatch = .complexity_order[["unknown"]]
    )
  ]
  stage_max <- global_cmplx[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  global_cmplx <- stage_max[global_cmplx, on = "stage"]
  global_cmplx[, dominant_in_stage := (complexity_rank == stage_max_rank)]
  global_cmplx[, stage_max_rank := NULL]

  list(
    by_stage = by_stage,
    raw = global_raw,
    summary = global_summary,
    complexity = global_cmplx
  )
}

#' @title Run all benchmarks
#' @description Backward-compatible flat runner for a pre-built benchmark
#'   catalog.
#' @param benchmarks A named list of benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A named list with elements raw, summary, and complexity.
run_all_benchmarks <- function(benchmarks, cfg) {
  .run_benchmark_set(benchmarks, cfg)
}
