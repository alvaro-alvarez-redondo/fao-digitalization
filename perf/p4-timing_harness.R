#' @title Timing harness module
#' @description Wall-clock timing utilities for benchmark execution,
#'   replication loops, and summary statistic aggregation.
#' @keywords internal
#' @noRd
NULL

# ── 4. timing harness ────────────────────────────────────────────────────────

#' @title Time function execution
#' @description Execute a zero-argument function once and return elapsed wall
#'   time in seconds.
#' @param fn A function with no required arguments.
#' @return A numeric scalar elapsed time in seconds.
time_fn <- function(fn) {
  t0 <- proc.time()[["elapsed"]]
  fn()
  proc.time()[["elapsed"]] - t0
}

#' @title Run benchmark loop
#' @description Time a benchmark function factory across input sizes and
#'   repetitions.
#' @param fn_factory A function taking n and returning a zero-argument function.
#' @param input_sizes An integer vector of input sizes.
#' @param n_reps An integer scalar number of repetitions per input size.
#' @param quiet A logical scalar. If TRUE, suppress message output.
#' @param progressor An optional progressr progressor function.
#' @return A data.table with columns n, rep, and elapsed_s.
run_benchmark <- function(
  fn_factory,
  input_sizes,
  n_reps = 5L,
  quiet = FALSE,
  progressor = NULL
) {
  results <- vector("list", length(input_sizes))
  n_sizes <- length(input_sizes)

  for (i in seq_along(input_sizes)) {
    n_i <- input_sizes[[i]]
    if (!is.null(progressor)) {
      progressor(sprintf("n = %d  (%d/%d)", n_i, i, n_sizes))
    } else if (!quiet) {
      message(sprintf("  n = %d ...", n_i))
    }

    # Build the benchmark closure once per input size so timing reflects
    # repeated execution cost of the target operation, not data setup cost.
    fn <- fn_factory(n_i)

    # Trigger collection once per n to limit cross-size carry-over without
    # polluting each timed repetition with GC overhead.
    gc(verbose = FALSE, full = FALSE)
    times <- numeric(n_reps)
    for (r in seq_len(n_reps)) {
      times[r] <- time_fn(fn)
    }

    results[[i]] <- data.table::data.table(
      n = rep(n_i, n_reps),
      rep = seq_len(n_reps),
      elapsed_s = times
    )
  }

  data.table::rbindlist(results)
}

#' @title Summarize benchmark results
#' @description Aggregate raw timing observations into per-size summary
#'   statistics.
#' @param raw_dt A data.table returned by run_benchmark().
#' @return A data.table with columns n, median_s, mean_s, sd_s, min_s, and max_s.
summarise_benchmark <- function(raw_dt) {
  raw_dt[,
    .(
      median_s = stats::median(elapsed_s),
      mean_s = mean(elapsed_s),
      sd_s = stats::sd(elapsed_s),
      min_s = min(elapsed_s),
      max_s = max(elapsed_s)
    ),
    by = n
  ][order(n)]
}
