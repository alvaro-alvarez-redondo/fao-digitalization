# module:      p4-timing_harness
# description: wall-clock timing harness for the perf module.
#   provides low-level timing (time_fn), the per-size benchmark loop
#   (run_benchmark), and the per-n summary statistics aggregator
#   (summarise_benchmark).
#
# sourced by:  perf/run_perf.R

# ── 4. timing harness ────────────────────────────────────────────────────────

#' @title time a function call
#' @description executes `fn()` once and returns elapsed wall-clock seconds.
#'   triggers a minor GC before timing to reduce noise from pending allocations.
#'   uses `proc.time()` which has sub-millisecond resolution on all platforms.
#' @param fn zero-argument function to time.
#' @return numeric scalar — elapsed seconds.
time_fn <- function(fn) {
  gc(verbose = FALSE, full = FALSE)
  t0 <- proc.time()[["elapsed"]]
  fn()
  proc.time()[["elapsed"]] - t0
}

#' @title run benchmark
#' @description times `fn` across all `input_sizes` with `n_reps` repetitions
#'   per size. generates fresh synthetic input before each repetition to avoid
#'   caching artefacts. returns a data.table of raw timing observations.
#' @param fn_factory function(n) that returns a zero-argument function to time.
#'   the inner function should capture all required inputs for one experiment
#'   via closure.
#' @param input_sizes integer vector of input sizes to benchmark over.
#' @param n_reps integer number of replications per size (default 5L).
#' @param quiet logical. suppress per-size progress messages (only applies
#'   when `progressor` is NULL).
#' @param progressor optional `progressr::progressor()` function. when supplied,
#'   one tick is emitted per input size and per-size `message()` output is
#'   suppressed. pass NULL to use message-based output controlled by `quiet`.
#' @return data.table with columns: n (int), rep (int), elapsed_s (dbl).
run_benchmark <- function(fn_factory, input_sizes, n_reps = 5L, quiet = FALSE,
                          progressor = NULL) {
  results <- vector("list", length(input_sizes))
  n_sizes <- length(input_sizes)

  for (i in seq_along(input_sizes)) {
    n_i <- input_sizes[[i]]
    if (!is.null(progressor)) {
      progressor(sprintf("n = %d  (%d/%d)", n_i, i, n_sizes))
    } else if (!quiet) {
      message(sprintf("  n = %d ...", n_i))
    }

    times <- numeric(n_reps)
    for (r in seq_len(n_reps)) {
      fn       <- fn_factory(n_i)
      times[r] <- time_fn(fn)
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
#' @description computes per-n summary statistics (median, mean, sd, min, max)
#'   from raw timing observations returned by `run_benchmark()`.
#' @param raw_dt data.table returned by `run_benchmark()`.
#' @return data.table with columns: n, median_s, mean_s, sd_s, min_s, max_s.
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
