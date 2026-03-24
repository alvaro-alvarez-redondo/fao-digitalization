# complexity_analysis/aggregation.R
# description: aggregation layer for the Big O complexity analysis module.
#   computes complexity ranks, stage-level dominance flags, and the overall
#   pipeline complexity class from per-function timing results.
#
#   function → stage mappings are enforced in the benchmark registry so every
#   measurement is traceable. this module aggregates those per-function results
#   up to stage and pipeline level.
#
# sourced by: complexity_analysis/run_analysis.R

# ── aggregation layer ────────────────────────────────────────────────────────

#' @title canonical complexity ordering
#' @description returns the canonical ordering of complexity class labels from
#'   fastest (O(1)) to slowest (ERROR). used for ranking and comparison across
#'   the entire analysis.
#' @return character vector of complexity labels in ascending severity order.
get_complexity_order <- function() {
  c("O(1)", "O(log n)", "O(n)", "O(n log n)", "O(n^2)", "O(n^3)",
    "unknown", "ERROR")
}

#' @title add stage dominance flags
#' @description annotates a per-function complexity data.table with:
#'   - `complexity_rank` (integer rank within the canonical ordering)
#'   - `dominant_in_stage` (TRUE for the worst-class function in each stage)
#' @param complexity_dt data.table with columns: fn_name, stage, best_class.
#' @return annotated data.table (modified in place and returned invisibly).
add_stage_dominance <- function(complexity_dt) {
  complexity_order <- get_complexity_order()
  complexity_dt[,
    complexity_rank := match(best_class, complexity_order, nomatch = 7L)
  ]
  stage_max <- complexity_dt[,
    .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
    by = stage
  ]
  complexity_dt <- stage_max[complexity_dt, on = "stage"]
  complexity_dt[, dominant_in_stage := (complexity_rank == stage_max_rank)]
  complexity_dt[, stage_max_rank := NULL]
  complexity_dt
}

#' @title get overall pipeline complexity class
#' @description derives the single worst-case complexity class across all
#'   pipeline stages. uses the canonical ordering from get_complexity_order().
#' @param complexity_dt annotated data.table returned by add_stage_dominance().
#' @return character scalar — overall pipeline complexity class label.
get_overall_pipeline_class <- function(complexity_dt) {
  complexity_order <- get_complexity_order()
  dom_dt <- complexity_dt[dominant_in_stage == TRUE]
  stage_summary <- dom_dt[,
    .(dominant_class = best_class[[1L]]),
    by = stage
  ]
  if (nrow(stage_summary) == 0L) return("unknown")
  all_ranks <- match(stage_summary$dominant_class, complexity_order, nomatch = 7L)
  stage_summary$dominant_class[[which.max(all_ranks)]]
}

#' @title build function-to-stage contribution table
#' @description returns a data.table showing, for each function, its stage,
#'   complexity class, rank, and whether it is the dominant contributor.
#'   useful for understanding the relative weight of each function within its
#'   pipeline stage.
#' @param complexity_dt annotated data.table returned by add_stage_dominance().
#' @return data.table with columns: fn_name, stage, best_class, complexity_rank,
#'   dominant_in_stage.
build_contribution_table <- function(complexity_dt) {
  complexity_dt[,
    .(fn_name, stage, best_class, complexity_rank, dominant_in_stage)
  ][order(stage, -complexity_rank, fn_name)]
}
