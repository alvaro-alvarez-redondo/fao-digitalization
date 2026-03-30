#' @title Reporting and persistence module
#' @description Console reporting, Markdown export, and analysis persistence
#'   helpers for the performance framework.
#' @keywords internal
#' @noRd
NULL

# ── 8. reporting and persistence ─────────────────────────────────────────────

# ── 8a. console reporters ─────────────────────────────────────────────────────

#' @title Print stage report
#' @description Print a formatted console report for a single stage diagnostic.
#' @param stage_diagnostic A named list from diagnose_stage().
#' @param stage_diagnostic A named list from diagnose_stage().
#' @return Invisible NULL.
print_stage_report <- function(stage_diagnostic) {
  sid <- stage_diagnostic$stage_id
  dom <- stage_diagnostic$dominant_class
  cdt <- stage_diagnostic$complexity_dt
  bns <- stage_diagnostic$bottlenecks
  sigs <- stage_diagnostic$optimization_signals

  cat("\n")
  cat(strrep("\u2500", 70L), "\n")
  cat(sprintf("  STAGE: %s\n", toupper(sid)))
  cat(strrep("\u2500", 70L), "\n")
  cat(sprintf("  Dominant complexity : %s\n", dom))
  cat(sprintf(
    "  Scaling behaviour   : %s\n",
    stage_diagnostic$scaling_description
  ))
  if (isTRUE(stage_diagnostic$is_concern)) {
    cat(
      "  \u26a0  CONCERN: This stage contains at least one super-linear function.\n"
    )
  }

  # per-function table
  w_fn <- max(nchar(cdt$fn_name), 12L) + 2L
  w_class <- max(nchar(cdt$best_class), 10L) + 2L
  w_r2 <- 10L
  w_flag <- 8L
  cat("\n")
  header <- sprintf(
    "  %-*s %-*s %*s %*s",
    w_fn,
    "FUNCTION",
    w_class,
    "COMPLEXITY",
    w_r2,
    "adj.R\u00b2",
    w_flag,
    "DOMINANT"
  )
  cat(header, "\n")
  cat(sprintf("  %s\n", strrep("-", nchar(header) - 2L)))

  for (i in seq_len(nrow(cdt))) {
    row <- cdt[i]
    r2 <- if (is.na(row$r_squared)) {
      "   N/A"
    } else {
      sprintf("%6.4f", row$r_squared)
    }
    flag <- if (isTRUE(row$dominant_in_stage)) "  \u2605" else ""
    cat(sprintf(
      "  %-*s %-*s %*s %*s\n",
      w_fn,
      row$fn_name,
      w_class,
      row$best_class,
      w_r2,
      r2,
      w_flag,
      flag
    ))
  }

  # top bottlenecks
  if (nrow(bns) > 0L) {
    cat(sprintf("\n  Top %d bottleneck(s):\n", nrow(bns)))
    for (i in seq_len(nrow(bns))) {
      cat(sprintf(
        "    %d. %s  [%s]\n",
        i,
        bns$fn_name[[i]],
        bns$best_class[[i]]
      ))
    }
  }

  # optimization signals
  cat("\n  Optimization signals:\n")
  for (nm in names(sigs)) {
    cat(sprintf("    \u2192 %s\n", sigs[[nm]]))
  }

  return(invisible(NULL))
}

#' @title Print global report
#' @description Print a formatted cross-stage summary from a global
#'   diagnostic object.
#' @param global_diagnostic A named list from build_global_diagnostic().
#' @param global_diagnostic A named list from build_global_diagnostic().
#' @return Invisible NULL.
print_global_report <- function(global_diagnostic) {
  gd <- global_diagnostic
  sdt <- gd$stage_summary

  cat("\n")
  cat(strrep("\u2550", 70L), "\n")
  cat("  WHEP PIPELINE \u2014 UNIFIED COMPLEXITY DIAGNOSTIC\n")
  cat(strrep("\u2550", 70L), "\n\n")

  cat("  CROSS-STAGE SUMMARY\n")
  cat(strrep("-", 50L), "\n")
  for (i in seq_len(nrow(sdt))) {
    concern <- if (isTRUE(sdt$is_concern[[i]])) "  \u26a0" else ""
    cat(sprintf(
      "  %-28s  %-15s%s\n",
      sdt$stage[[i]],
      sdt$dominant_class[[i]],
      concern
    ))
  }

  cat("\n")
  cat(sprintf("  OVERALL PIPELINE CLASS : %s\n", gd$overall_class))
  cat(sprintf(
    "  Scaling behaviour      : %s\n",
    gd$overall_scaling_description
  ))
  if (!is.na(gd$pipeline_bottleneck)) {
    cat(sprintf("  Primary bottleneck     : %s\n", gd$pipeline_bottleneck))
  }
  cat(strrep("\u2550", 70L), "\n\n")

  return(invisible(NULL))
}

#' @title Print flat complexity report
#' @description Print a backward-compatible flat report from complexity rows.
#' @param complexity_dt A data.table from run_all_benchmarks()$complexity.
#' @return Invisible NULL.
print_complexity_report <- function(complexity_dt) {
  cat("\n")
  cat(strrep("\u2500", 70L), "\n")
  cat("  WHEP PIPELINE \u2014 EMPIRICAL BIG O ANALYSIS\n")
  cat(strrep("\u2500", 70L), "\n\n")

  w_fn <- max(nchar(complexity_dt$fn_name), 12L) + 2L
  w_stage <- max(nchar(complexity_dt$stage), 10L) + 2L
  w_class <- max(nchar(complexity_dt$best_class), 10L) + 2L
  w_r2 <- 10L
  w_flag <- 8L

  header <- sprintf(
    "%-*s %-*s %-*s %*s %*s",
    w_fn,
    "FUNCTION",
    w_stage,
    "STAGE",
    w_class,
    "COMPLEXITY",
    w_r2,
    "adj.R\u00b2",
    w_flag,
    "DOMINANT"
  )
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")

  for (i in seq_len(nrow(complexity_dt))) {
    row <- complexity_dt[i]
    r2 <- if (is.na(row$r_squared)) {
      "   N/A"
    } else {
      sprintf("%6.4f", row$r_squared)
    }
    flag <- if (isTRUE(row$dominant_in_stage)) "  \u2605" else ""
    cat(sprintf(
      "%-*s %-*s %-*s %*s %*s\n",
      w_fn,
      row$fn_name,
      w_stage,
      row$stage,
      w_class,
      row$best_class,
      w_r2,
      r2,
      w_flag,
      flag
    ))
  }

  cat("\n")
  cat(strrep("\u2500", 70L), "\n")
  cat("  PER-STAGE DOMINANT COMPLEXITY\n")
  cat(strrep("\u2500", 70L), "\n")

  complexity_order_vec <- names(.complexity_order)
  stage_summary <- complexity_dt[
    isTRUE(dominant_in_stage) | dominant_in_stage == TRUE,
    .(dominant_class = {
      classes <- sort(unique(best_class))
      ranked <- match(classes, complexity_order_vec, nomatch = 7L)
      classes[[which.max(ranked)]]
    }),
    by = stage
  ][order(stage)]

  for (i in seq_len(nrow(stage_summary))) {
    cat(sprintf(
      "  %-25s  %s\n",
      stage_summary$stage[[i]],
      stage_summary$dominant_class[[i]]
    ))
  }

  all_ranks <- match(
    stage_summary$dominant_class,
    complexity_order_vec,
    nomatch = 7L
  )
  overall <- stage_summary$dominant_class[[which.max(all_ranks)]]
  cat("\n")
  cat(sprintf("  OVERALL PIPELINE ESTIMATE: %s\n", overall))
  cat(strrep("\u2500", 70L), "\n\n")

  return(invisible(NULL))
}

# ── 8b. Markdown export ───────────────────────────────────────────────────────

# Internal reporting constants used by markdown metric construction.
.reporting_runtime_sample_n <- as.integer(c(1000L, 10000L, 50000L))
.reporting_expensive_rank_threshold <- .complexity_order[["O(n^2)"]]
.reporting_high_impact_threshold <- 0.35
.reporting_plaintext_max_col_width <- 52L

#' @title Sanitize markdown cell text
#' @description Replace control characters and table delimiters to keep
#'   generated markdown tables structurally valid.
#' @param text Character vector to sanitize.
#' @return Character vector safe for markdown table cells.
#' @keywords internal
#' @noRd
.sanitize_markdown_cell <- function(text) {
  if (length(text) == 0L) {
    return(character(0))
  }
  out <- as.character(text)
  out[is.na(out)] <- "N/A"
  out <- gsub("[\\r\\n\\t]+", " ", out, perl = TRUE)
  out <- gsub("\\|", "\\\\|", out, perl = TRUE)
  trimws(out)
}

#' @title Sanitize plaintext cell text
#' @description Replace control characters and vertical bars to keep plaintext
#'   matrix rendering deterministic and easy to scan in raw markdown.
#' @param text Character vector to sanitize.
#' @return Character vector safe for plaintext matrix cells.
#' @keywords internal
#' @noRd
.sanitize_plaintext_cell <- function(text) {
  if (length(text) == 0L) {
    return(character(0))
  }
  out <- as.character(text)
  out[is.na(out)] <- "N/A"
  out <- gsub("[\\r\\n\\t]+", " ", out, perl = TRUE)
  out <- gsub("\\|", "/", out, perl = TRUE)
  trimws(out)
}

#' @title Truncate plaintext cell text
#' @description Truncate cell content to a fixed display width to prevent
#'   overly wide matrices in raw markdown.
#' @param text Character vector to truncate.
#' @param width Integer scalar maximum display width.
#' @return Character vector.
#' @keywords internal
#' @noRd
.truncate_plaintext_cell <- function(text, width) {
  out <- as.character(text)
  too_wide <- nchar(out, type = "width") > width
  if (any(too_wide)) {
    keep <- max(1L, width - 1L)
    out[too_wide] <- paste0(substr(out[too_wide], 1L, keep), "~")
  }
  out
}

#' @title Build plaintext matrix lines
#' @description Render a data.frame-like object as a fixed-width ASCII matrix
#'   inside a fenced text block for raw markdown readability.
#' @param data Data frame or data.table with matrix values.
#' @param headers Character vector of display column headers.
#' @param max_col_width Integer scalar max display width per column.
#' @return Character vector of markdown lines.
#' @keywords internal
#' @noRd
.build_plaintext_matrix <- function(
  data,
  headers = names(data),
  max_col_width = .reporting_plaintext_max_col_width
) {
  dt <- data.table::as.data.table(data)

  if (length(headers) != ncol(dt)) {
    stop("headers length must match number of columns in data")
  }

  if (nrow(dt) == 0L) {
    placeholder <- as.list(rep("N/A", ncol(dt)))
    names(placeholder) <- names(dt)
    dt <- data.table::as.data.table(placeholder)
  }

  header_values <- .truncate_plaintext_cell(
    .sanitize_plaintext_cell(headers),
    width = max_col_width
  )

  cell_columns <- lapply(seq_len(ncol(dt)), function(i) {
    .truncate_plaintext_cell(
      .sanitize_plaintext_cell(dt[[i]]),
      width = max_col_width
    )
  })

  widths <- vapply(seq_len(ncol(dt)), function(i) {
    max(nchar(c(header_values[[i]], cell_columns[[i]]), type = "width"))
  }, integer(1))

  format_row <- function(values) {
    padded <- mapply(
      function(value, width) sprintf(paste0("%-", width, "s"), value),
      values,
      widths,
      SIMPLIFY = TRUE,
      USE.NAMES = FALSE
    )
    paste0("| ", paste(padded, collapse = " | "), " |")
  }

  header_line <- format_row(header_values)
  divider_line <- paste0(
    "| ",
    paste(vapply(widths, function(w) strrep("-", w), character(1)), collapse = " | "),
    " |"
  )
  row_lines <- vapply(seq_len(nrow(dt)), function(i) {
    row_values <- vapply(cell_columns, function(col) col[[i]], character(1))
    format_row(row_values)
  }, character(1))

  c("```text", header_line, divider_line, row_lines, "```")
}

#' @title Format numeric metric values
#' @description Format numeric vectors with deterministic output and explicit
#'   N/A handling.
#' @param x Numeric vector.
#' @param digits Integer scalar number of significant digits.
#' @return Character vector.
#' @keywords internal
#' @noRd
.format_metric_value <- function(x, digits = 6L) {
  out <- rep("N/A", length(x))
  valid <- !is.na(x) & is.finite(x)
  if (any(valid)) {
    out[valid] <- formatC(x[valid], format = "fg", digits = digits)
  }
  out
}

#' @title Build reporting metrics for markdown
#' @description Compute stage-level, function-level, and chart-ready metrics
#'   consumed by build_analysis_markdown().
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @return A named list of data.tables and scalar summary fields.
#' @keywords internal
#' @noRd
prepare_reporting_metrics <- function(results) {
  if (!is.list(results) || !"complexity" %in% names(results)) {
    stop("results must be a list containing a 'complexity' data.table")
  }

  complexity_dt <- data.table::as.data.table(results$complexity)
  summary_dt <- if ("summary" %in% names(results)) {
    data.table::as.data.table(results$summary)
  } else {
    data.table::data.table()
  }

  if (nrow(complexity_dt) == 0L) {
    stage_summary_dt <- data.table::data.table(
      stage = character(0),
      function_count = integer(0),
      max_complexity_rank = integer(0),
      max_complexity = character(0),
      expensive_function_count = integer(0),
      expensive_function_pct = numeric(0),
      stage_runtime_total_s = numeric(0),
      stage_runtime_proportion = numeric(0),
      bottleneck_flag = character(0)
    )

    return(list(
      function_metrics = complexity_dt,
      stage_summary = stage_summary_dt,
      runtime_projection = data.table::data.table(),
      sample_n_values = .reporting_runtime_sample_n,
      chart_complexity = data.table::data.table(),
      chart_stage_runtime = data.table::data.table(),
      chart_slope = data.table::data.table(),
      overall_class = "unknown",
      stage_bottleneck_runtime = NA_character_,
      stage_bottleneck_complexity = NA_character_,
      function_bottleneck = NA_character_,
      function_bottleneck_stage = NA_character_
    ))
  }

  required_cols <- c(
    "fn_name",
    "stage",
    "description",
    "best_class",
    "r_squared",
    "slope_per_n"
  )
  missing_cols <- setdiff(required_cols, names(complexity_dt))
  if (length(missing_cols) > 0L) {
    stop(sprintf(
      "results$complexity is missing required columns: %s",
      paste(missing_cols, collapse = ", ")
    ))
  }

  if (!"complexity_rank" %in% names(complexity_dt)) {
    complexity_dt[,
      complexity_rank := match(
        best_class,
        names(.complexity_order),
        nomatch = .complexity_order[["unknown"]]
      )
    ]
  }

  if (!"dominant_in_stage" %in% names(complexity_dt)) {
    stage_max <- complexity_dt[,
      .(stage_max_rank = max(complexity_rank, na.rm = TRUE)),
      by = stage
    ]
    complexity_dt <- stage_max[complexity_dt, on = "stage"]
    complexity_dt[, dominant_in_stage := (complexity_rank == stage_max_rank)]
    complexity_dt[, stage_max_rank := NULL]
  }

  has_runtime_summary <- nrow(summary_dt) > 0L &&
    all(c("fn_name", "stage", "n", "median_s") %in% names(summary_dt))

  if (has_runtime_summary) {
    fn_runtime_dt <- summary_dt[,
      .(
        observed_runtime_s = sum(median_s, na.rm = TRUE),
        runtime_at_max_n_s = median_s[[which.max(n)]]
      ),
      by = .(fn_name, stage)
    ]
    n_candidates <- sort(unique(as.integer(summary_dt$n[
      is.finite(summary_dt$n) & summary_dt$n > 0
    ])))
  } else {
    fn_runtime_dt <- complexity_dt[, .(fn_name, stage)]
    fn_runtime_dt[, `:=`(
      observed_runtime_s = NA_real_,
      runtime_at_max_n_s = NA_real_
    )]
    n_candidates <- integer(0)
  }

  sample_n_values <- if (length(n_candidates) == 0L) {
    .reporting_runtime_sample_n
  } else {
    idx <- unique(as.integer(round(stats::quantile(
      seq_along(n_candidates),
      probs = c(0, 0.5, 1),
      type = 1
    ))))
    as.integer(n_candidates[idx])
  }

  fn_metrics <- fn_runtime_dt[complexity_dt, on = c("fn_name", "stage")]
  fn_metrics[is.na(observed_runtime_s), observed_runtime_s := 0]
  fn_metrics[
    is.na(runtime_at_max_n_s),
    runtime_at_max_n_s := observed_runtime_s
  ]

  stage_runtime_dt <- fn_metrics[,
    .(
      stage_runtime_total_s = sum(observed_runtime_s, na.rm = TRUE)
    ),
    by = stage
  ]
  fn_metrics <- stage_runtime_dt[fn_metrics, on = "stage"]
  fn_metrics[,
    relative_impact := data.table::fifelse(
      stage_runtime_total_s > 0,
      observed_runtime_s / stage_runtime_total_s,
      0
    )
  ]

  fn_metrics[,
    is_expensive := complexity_rank >= .reporting_expensive_rank_threshold
  ]
  fn_metrics[,
    scaling_flag := data.table::fifelse(
      is_expensive & relative_impact >= .reporting_high_impact_threshold,
      "critical",
      data.table::fifelse(
        is_expensive,
        "high_complexity",
        data.table::fifelse(
          relative_impact >= .reporting_high_impact_threshold,
          "high_impact",
          "ok"
        )
      )
    )
  ]
  fn_metrics[,
    visual_indicator := data.table::fifelse(
      scaling_flag == "critical",
      "!!!",
      data.table::fifelse(
        scaling_flag == "high_complexity",
        "!!",
        data.table::fifelse(
          scaling_flag == "high_impact",
          "!",
          "."
        )
      )
    )
  ]

  runtime_projection_dt <- fn_metrics[,
    .(n_sample = sample_n_values),
    by = .(fn_name, stage, slope_per_n)
  ]
  runtime_projection_dt[,
    estimated_runtime_s := data.table::fifelse(
      !is.na(slope_per_n) & is.finite(slope_per_n) & slope_per_n >= 0,
      slope_per_n * n_sample,
      NA_real_
    )
  ]
  runtime_projection_text_dt <- runtime_projection_dt[,
    .(
      estimated_runtime_summary = paste(
        sprintf(
          "n=%d: %s s",
          n_sample,
          .format_metric_value(estimated_runtime_s, digits = 4L)
        ),
        collapse = "; "
      )
    ),
    by = .(fn_name, stage)
  ]

  fn_metrics <- runtime_projection_text_dt[
    fn_metrics,
    on = c("fn_name", "stage")
  ]

  stage_summary_dt <- fn_metrics[,
    {
      valid_rank <- complexity_rank[!is.na(complexity_rank)]
      max_rank <- if (length(valid_rank) > 0L) {
        max(valid_rank)
      } else {
        .complexity_order[["unknown"]]
      }
      max_idx <- which.max(data.table::fifelse(
        is.na(complexity_rank),
        -Inf,
        complexity_rank
      ))
      max_complexity <- if (
        length(max_idx) == 0L || is.na(best_class[[max_idx]])
      ) {
        "unknown"
      } else {
        best_class[[max_idx]]
      }

      .(
        function_count = .N,
        max_complexity_rank = as.integer(max_rank),
        max_complexity = max_complexity,
        expensive_function_count = as.integer(sum(is_expensive, na.rm = TRUE)),
        stage_runtime_total_s = sum(observed_runtime_s, na.rm = TRUE)
      )
    },
    by = stage
  ]

  pipeline_total_runtime <- sum(
    stage_summary_dt$stage_runtime_total_s,
    na.rm = TRUE
  )
  stage_summary_dt[,
    expensive_function_pct := data.table::fifelse(
      function_count > 0,
      100 * expensive_function_count / function_count,
      0
    )
  ]
  stage_summary_dt[,
    stage_runtime_proportion := if (pipeline_total_runtime > 0) {
      stage_runtime_total_s / pipeline_total_runtime
    } else {
      0
    }
  ]

  runtime_max <- if (nrow(stage_summary_dt) > 0L) {
    max(stage_summary_dt$stage_runtime_proportion, na.rm = TRUE)
  } else {
    NA_real_
  }
  complexity_max <- if (nrow(stage_summary_dt) > 0L) {
    max(stage_summary_dt$max_complexity_rank, na.rm = TRUE)
  } else {
    NA_real_
  }

  stage_summary_dt[,
    bottleneck_flag := data.table::fifelse(
      abs(stage_runtime_proportion - runtime_max) < .Machine$double.eps^0.5 |
        max_complexity_rank == complexity_max,
      "bottleneck",
      "normal"
    )
  ]

  dom_classes <- stage_summary_dt$max_complexity
  dom_ranks <- match(
    dom_classes,
    names(.complexity_order),
    nomatch = .complexity_order[["unknown"]]
  )
  overall_class <- dom_classes[[which.max(dom_ranks)]]

  chart_complexity_dt <- fn_metrics[, .(
    stage,
    fn_name,
    best_class,
    complexity_rank,
    visual_indicator
  )][order(stage, -complexity_rank, fn_name)]

  chart_stage_runtime_dt <- stage_summary_dt[, .(
    stage,
    stage_runtime_total_s,
    stage_runtime_proportion,
    function_count,
    expensive_function_pct,
    max_complexity_rank,
    max_complexity
  )][order(-stage_runtime_proportion, -max_complexity_rank, stage)]

  chart_slope_dt <- fn_metrics[, .(
    stage,
    fn_name,
    slope_per_n,
    relative_impact,
    scaling_flag,
    visual_indicator
  )][order(-relative_impact, -slope_per_n, stage, fn_name)]

  stage_bottleneck_runtime <- if (nrow(chart_stage_runtime_dt) > 0L) {
    chart_stage_runtime_dt$stage[[1L]]
  } else {
    NA_character_
  }
  stage_bottleneck_complexity <- if (nrow(stage_summary_dt) > 0L) {
    stage_summary_dt[order(-max_complexity_rank, stage)]$stage[[1L]]
  } else {
    NA_character_
  }

  fn_bottleneck_row <- fn_metrics[order(
    -relative_impact,
    -complexity_rank,
    fn_name
  )][1L]
  function_bottleneck <- if (nrow(fn_bottleneck_row) > 0L) {
    fn_bottleneck_row$fn_name[[1L]]
  } else {
    NA_character_
  }
  function_bottleneck_stage <- if (nrow(fn_bottleneck_row) > 0L) {
    fn_bottleneck_row$stage[[1L]]
  } else {
    NA_character_
  }

  list(
    function_metrics = fn_metrics,
    stage_summary = stage_summary_dt,
    runtime_projection = runtime_projection_dt,
    sample_n_values = sample_n_values,
    chart_complexity = chart_complexity_dt,
    chart_stage_runtime = chart_stage_runtime_dt,
    chart_slope = chart_slope_dt,
    overall_class = overall_class,
    stage_bottleneck_runtime = stage_bottleneck_runtime,
    stage_bottleneck_complexity = stage_bottleneck_complexity,
    function_bottleneck = function_bottleneck,
    function_bottleneck_stage = function_bottleneck_stage
  )
}

#' @title Build analysis Markdown lines
#' @description Build a structured Markdown summary of performance analysis
#'   output using vectorized text operations.
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @return Character vector of markdown lines.
build_analysis_markdown <- function(results) {
  md <- prepare_reporting_metrics(results)
  fn_metrics <- data.table::copy(md$function_metrics)
  stage_summary <- data.table::copy(md$stage_summary)
  chart_complexity <- data.table::copy(md$chart_complexity)
  chart_stage_runtime <- data.table::copy(md$chart_stage_runtime)
  chart_slope <- data.table::copy(md$chart_slope)
  runtime_projection <- data.table::copy(md$runtime_projection)

  if (nrow(stage_summary) > 0L) {
    stage_summary[,
      expensive_share_label := sprintf("%.1f%%", expensive_function_pct)
    ]
    stage_summary[,
      runtime_share_label := sprintf("%.1f%%", 100 * stage_runtime_proportion)
    ]
    stage_summary[,
      stage_runtime_total_label := .format_metric_value(
        stage_runtime_total_s,
        digits = 5L
      )
    ]

    stage_matrix_dt <- stage_summary[, .(
      Stage = stage,
      `Total functions` = as.character(function_count),
      `Max complexity` = max_complexity,
      `Expensive functions` = as.character(expensive_function_count),
      `Expensive share` = expensive_share_label,
      `Stage runtime total (s)` = stage_runtime_total_label,
      `Stage runtime share` = runtime_share_label,
      `Bottleneck status` = bottleneck_flag
    )]
  } else {
    stage_matrix_dt <- data.table::data.table(
      Stage = "N/A",
      `Total functions` = "0",
      `Max complexity` = "unknown",
      `Expensive functions` = "0",
      `Expensive share` = "0.0%",
      `Stage runtime total (s)` = "N/A",
      `Stage runtime share` = "0.0%",
      `Bottleneck status` = "normal"
    )
  }

  if (nrow(fn_metrics) > 0L) {
    fn_metrics[,
      r_squared_label := .format_metric_value(r_squared, digits = 6L)
    ]
    fn_metrics[, slope_label := .format_metric_value(slope_per_n, digits = 6L)]
    fn_metrics[,
      relative_impact_label := sprintf("%.1f%%", 100 * relative_impact)
    ]
    fn_metrics[,
      dominant_stage_label := data.table::fifelse(
        !is.na(dominant_in_stage) & dominant_in_stage,
        "yes",
        "no"
      )
    ]
    fn_metrics[,
      indicator_label := sprintf("%s (%s)", visual_indicator, scaling_flag)
    ]

    function_matrix_dt <- fn_metrics[, .(
      Function = fn_name,
      Stage = stage,
      Description = description,
      Complexity = best_class,
      `adj.R2` = r_squared_label,
      `Slope per n` = slope_label,
      `Estimated runtime (sample n)` = estimated_runtime_summary,
      `Relative impact` = relative_impact_label,
      Flags = indicator_label,
      `Dominant in stage` = dominant_stage_label
    )]
  } else {
    function_matrix_dt <- data.table::data.table(
      Function = "N/A",
      Stage = "N/A",
      Description = "N/A",
      Complexity = "unknown",
      `adj.R2` = "N/A",
      `Slope per n` = "N/A",
      `Estimated runtime (sample n)` = "N/A",
      `Relative impact` = "0.0%",
      Flags = ". (ok)",
      `Dominant in stage` = "no"
    )
  }

  if (nrow(chart_complexity) > 0L) {
    complexity_chart_matrix_dt <- chart_complexity[, .(
      Stage = stage,
      Function = fn_name,
      Complexity = best_class,
      `Complexity rank` = as.character(complexity_rank),
      Indicator = visual_indicator
    )]
  } else {
    complexity_chart_matrix_dt <- data.table::data.table(
      Stage = "N/A",
      Function = "N/A",
      Complexity = "unknown",
      `Complexity rank` = "7",
      Indicator = "."
    )
  }

  if (nrow(chart_stage_runtime) > 0L) {
    chart_stage_runtime[,
      runtime_total_label := .format_metric_value(
        stage_runtime_total_s,
        digits = 5L
      )
    ]
    chart_stage_runtime[,
      runtime_share_label := sprintf("%.1f%%", 100 * stage_runtime_proportion)
    ]
    chart_stage_runtime[,
      expensive_share_label := sprintf("%.1f%%", expensive_function_pct)
    ]

    stage_runtime_chart_matrix_dt <- chart_stage_runtime[, .(
      Stage = stage,
      `Runtime total (s)` = runtime_total_label,
      `Runtime share` = runtime_share_label,
      `Function count` = as.character(function_count),
      `Expensive share` = expensive_share_label,
      `Max complexity` = max_complexity
    )]
  } else {
    stage_runtime_chart_matrix_dt <- data.table::data.table(
      Stage = "N/A",
      `Runtime total (s)` = "N/A",
      `Runtime share` = "0.0%",
      `Function count` = "0",
      `Expensive share` = "0.0%",
      `Max complexity` = "unknown"
    )
  }

  if (nrow(chart_slope) > 0L) {
    chart_slope[, slope_label := .format_metric_value(slope_per_n, digits = 6L)]
    chart_slope[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]

    slope_chart_matrix_dt <- chart_slope[, .(
      Stage = stage,
      Function = fn_name,
      `Slope per n` = slope_label,
      `Relative impact` = impact_label,
      `Scaling flag` = scaling_flag,
      Indicator = visual_indicator
    )]
  } else {
    slope_chart_matrix_dt <- data.table::data.table(
      Stage = "N/A",
      Function = "N/A",
      `Slope per n` = "N/A",
      `Relative impact` = "0.0%",
      `Scaling flag` = "ok",
      Indicator = "."
    )
  }

  if (nrow(runtime_projection) > 0L) {
    runtime_projection[,
      estimate_label := .format_metric_value(estimated_runtime_s, digits = 5L)
    ]

    projection_matrix_dt <- runtime_projection[, .(
      Stage = stage,
      Function = fn_name,
      n = as.character(n_sample),
      `Estimated runtime (s)` = estimate_label
    )]
  } else {
    projection_matrix_dt <- data.table::data.table(
      Stage = "N/A",
      Function = "N/A",
      n = "0",
      `Estimated runtime (s)` = "N/A"
    )
  }

  sample_n_label <- paste(md$sample_n_values, collapse = ", ")

  c(
    "# Performance Analysis Summary",
    "",
    sprintf(
      "- Analysis timestamp (UTC): %s",
      format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    ),
    sprintf("- Overall pipeline class: %s", md$overall_class),
    sprintf("- Runtime bottleneck stage: %s", md$stage_bottleneck_runtime),
    sprintf(
      "- Complexity bottleneck stage: %s",
      md$stage_bottleneck_complexity
    ),
    sprintf(
      "- Primary function bottleneck: %s (%s)",
      md$function_bottleneck,
      md$function_bottleneck_stage
    ),
    sprintf("- Runtime projection sample n values: %s", sample_n_label),
    "",
    "## Stage Summary",
    "",
    .build_plaintext_matrix(stage_matrix_dt),
    "",
    "## Function Complexity",
    "",
    .build_plaintext_matrix(function_matrix_dt),
    "",
    "## Chart Data: Function Complexities",
    "",
    .build_plaintext_matrix(complexity_chart_matrix_dt),
    "",
    "## Chart Data: Stage Runtime Proportions",
    "",
    .build_plaintext_matrix(stage_runtime_chart_matrix_dt),
    "",
    "## Chart Data: Slope per n and Relative Impact",
    "",
    .build_plaintext_matrix(slope_chart_matrix_dt),
    "",
    "## Runtime Projections by Sample n",
    "",
    .build_plaintext_matrix(projection_matrix_dt)
  )
}

#' @title Order stage IDs for reporting
#' @description Order stage identifiers using canonical stage order first,
#'   followed by non-canonical stages in sorted order.
#' @param stage_ids A character vector of stage identifiers.
#' @return A character vector of ordered stage identifiers.
#' @keywords internal
#' @noRd
.order_stage_ids_for_reports <- function(stage_ids) {
  stage_ids <- unique(as.character(stage_ids))
  stage_ids <- stage_ids[!is.na(stage_ids) & nzchar(stage_ids)]

  canonical_ids <- names(get_perf_pipeline_report_file_map())
  c(
    intersect(canonical_ids, stage_ids),
    sort(setdiff(stage_ids, canonical_ids))
  )
}

#' @title Resolve stage report filename
#' @description Resolve stage-specific markdown filename from canonical map, or
#'   derive a sanitized fallback filename for non-canonical stages, always prefixed with 'perf_'.
#' @param stage_id A character scalar stage identifier.
#' @return A character scalar markdown filename.
#' @keywords internal
#' @noRd
.resolve_stage_report_filename <- function(stage_id) {
  stage_id <- as.character(stage_id)
  file_map <- get_perf_pipeline_report_file_map()
  resolved <- unname(file_map[stage_id])

  missing_idx <- is.na(resolved) | !nzchar(resolved)
  if (any(missing_idx)) {
    safe_stage <- gsub("[^A-Za-z0-9_-]+", "_", stage_id[missing_idx])
    resolved[missing_idx] <- paste0("perf_", safe_stage, "_pipeline.md")
  }

  resolved
}

#' @title Resolve stage report label
#' @description Resolve stage-specific report label used in markdown headers.
#' @param stage_id A character scalar stage identifier.
#' @return A character scalar stage report label.
#' @keywords internal
#' @noRd
.resolve_stage_report_label <- function(stage_id) {
  sub("\\.md$", "", .resolve_stage_report_filename(stage_id))
}

#' @title Build ASCII bars
#' @description Build deterministic ASCII bars from normalized proportions.
#' @param x Numeric vector of values expected in [0, 1].
#' @param width Integer scalar bar width.
#' @return Character vector of fixed-width ASCII bars.
#' @keywords internal
#' @noRd
.build_ascii_bar <- function(x, width = 24L) {
  x <- as.numeric(x)
  safe_x <- data.table::fifelse(is.na(x) | !is.finite(x), 0, x)
  safe_x <- pmin(1, pmax(0, safe_x))
  filled <- as.integer(round(width * safe_x))

  vapply(
    filled,
    function(k) {
      paste0(strrep("#", k), strrep("-", width - k))
    },
    character(1)
  )
}

#' @title Build pipeline markdown from reporting metrics
#' @description Build one ultra-detailed markdown report for a single pipeline
#'   stage from precomputed reporting metrics.
#' @param metrics A list returned by prepare_reporting_metrics().
#' @param stage_id A character scalar stage identifier.
#' @return Character vector of markdown lines.
#' @keywords internal
#' @noRd
.build_pipeline_markdown_from_metrics <- function(metrics, stage_id) {
  stage_label <- .resolve_stage_report_label(stage_id)

  stage_summary <- data.table::copy(metrics$stage_summary[stage == stage_id])
  fn_metrics <- data.table::copy(metrics$function_metrics[stage == stage_id])
  runtime_projection <- data.table::copy(metrics$runtime_projection[
    stage == stage_id
  ])

  if (nrow(stage_summary) == 0L) {
    stage_summary <- data.table::data.table(
      stage = stage_id,
      function_count = 0L,
      max_complexity_rank = .complexity_order[["unknown"]],
      max_complexity = "unknown",
      expensive_function_count = 0L,
      expensive_function_pct = 0,
      stage_runtime_total_s = 0,
      stage_runtime_proportion = 0,
      bottleneck_flag = "normal"
    )
  }

  if (nrow(fn_metrics) > 0L) {
    data.table::setorder(
      fn_metrics,
      -relative_impact,
      -complexity_rank,
      fn_name
    )

    fn_metrics[,
      r_squared_label := .format_metric_value(r_squared, digits = 6L)
    ]
    fn_metrics[, slope_label := .format_metric_value(slope_per_n, digits = 6L)]
    fn_metrics[,
      estimate_label := .sanitize_plaintext_cell(estimated_runtime_summary)
    ]
    fn_metrics[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]
    fn_metrics[,
      indicator_label := sprintf("%s (%s)", visual_indicator, scaling_flag)
    ]
    fn_metrics[,
      bottleneck_label := data.table::fifelse(
        scaling_flag == "critical",
        "yes",
        "no"
      )
    ]

    function_matrix_dt <- fn_metrics[, .(
      Function = fn_name,
      Description = description,
      Complexity = best_class,
      `adj.R2` = r_squared_label,
      `Slope per n` = slope_label,
      `Estimated runtime (sample n)` = estimate_label,
      `Relative impact` = impact_label,
      Indicator = indicator_label,
      Bottleneck = bottleneck_label
    )]

    highest_complexity_row <- fn_metrics[order(
      -complexity_rank,
      -relative_impact,
      fn_name
    )][1L]
    primary_bottleneck_row <- fn_metrics[order(
      -relative_impact,
      -complexity_rank,
      fn_name
    )][1L]

    complexity_dist <- fn_metrics[, .(function_count = .N), by = best_class]
    complexity_dist[,
      complexity_rank := match(
        best_class,
        names(.complexity_order),
        nomatch = .complexity_order[["unknown"]]
      )
    ]
    complexity_dist <- complexity_dist[order(
      -function_count,
      -complexity_rank,
      best_class
    )]
    complexity_dist[, complexity_rank := NULL]
    complexity_dist[, share := function_count / sum(function_count)]
    complexity_dist[, share_label := sprintf("%.1f%%", 100 * share)]
    complexity_dist[, share_bar := .build_ascii_bar(share, width = 24L)]

    complexity_dist_matrix_dt <- complexity_dist[, .(
      `Complexity class` = best_class,
      `Function count` = as.character(function_count),
      Share = share_label,
      Distribution = share_bar
    )]

    runtime_dist <- fn_metrics[, .(fn_name, relative_impact)]
    data.table::setorder(runtime_dist, -relative_impact, fn_name)
    runtime_dist[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]
    runtime_dist[, impact_bar := .build_ascii_bar(relative_impact, width = 24L)]

    runtime_dist_matrix_dt <- runtime_dist[, .(
      Function = fn_name,
      `Relative impact` = impact_label,
      Distribution = impact_bar
    )]

    bottleneck_candidates <- fn_metrics[scaling_flag != "ok"]
    if (nrow(bottleneck_candidates) == 0L) {
      bottleneck_candidates <- fn_metrics
    }
    data.table::setorder(
      bottleneck_candidates,
      -relative_impact,
      -complexity_rank,
      fn_name
    )
    bottleneck_candidates <- bottleneck_candidates[seq_len(min(
      3L,
      nrow(bottleneck_candidates)
    ))]
    bottleneck_candidates[,
      impact_label := sprintf("%.1f%%", 100 * relative_impact)
    ]

    if (nrow(bottleneck_candidates) > 0L) {
      bottleneck_lines <- sprintf(
        "- %s: class %s, impact %s, flag %s.",
        .sanitize_plaintext_cell(bottleneck_candidates$fn_name),
        .sanitize_plaintext_cell(bottleneck_candidates$best_class),
        bottleneck_candidates$impact_label,
        .sanitize_plaintext_cell(bottleneck_candidates$scaling_flag)
      )
    } else {
      bottleneck_lines <- "- None identified."
    }
  } else {
    function_matrix_dt <- data.table::data.table(
      Function = "N/A",
      Description = "N/A",
      Complexity = "unknown",
      `adj.R2` = "N/A",
      `Slope per n` = "N/A",
      `Estimated runtime (sample n)` = "N/A",
      `Relative impact` = "0.0%",
      Indicator = ". (ok)",
      Bottleneck = "no"
    )
    complexity_dist_matrix_dt <- data.table::data.table(
      `Complexity class` = "unknown",
      `Function count` = "0",
      Share = "0.0%",
      Distribution = strrep("-", 24L)
    )
    runtime_dist_matrix_dt <- data.table::data.table(
      Function = "N/A",
      `Relative impact` = "0.0%",
      Distribution = strrep("-", 24L)
    )
    bottleneck_lines <- "- None identified."

    highest_complexity_row <- data.table::data.table(
      fn_name = "N/A",
      best_class = "unknown"
    )
    primary_bottleneck_row <- data.table::data.table(
      fn_name = "N/A",
      best_class = "unknown",
      relative_impact = 0
    )
  }

  runtime_projection <- runtime_projection[order(n_sample, fn_name)]
  if (nrow(runtime_projection) > 0L) {
    runtime_projection[,
      estimate_label := .format_metric_value(estimated_runtime_s, digits = 5L)
    ]

    projection_matrix_dt <- runtime_projection[, .(
      Function = fn_name,
      n = as.character(n_sample),
      `Estimated runtime (s)` = estimate_label
    )]
  } else {
    projection_matrix_dt <- data.table::data.table(
      Function = "N/A",
      n = "0",
      `Estimated runtime (s)` = "N/A"
    )
  }

  sample_n_label <- paste(metrics$sample_n_values, collapse = ", ")
  runtime_share_label <- sprintf(
    "%.1f%%",
    100 * stage_summary$stage_runtime_proportion[[1L]]
  )

  c(
    sprintf("# Pipeline Performance Report: %s", stage_label),
    "",
    sprintf("- Stage identifier: %s", stage_id),
    sprintf(
      "- Total functions benchmarked: %d",
      stage_summary$function_count[[1L]]
    ),
    sprintf(
      "- Dominant complexity class: %s",
      stage_summary$max_complexity[[1L]]
    ),
    sprintf("- Stage runtime share: %s", runtime_share_label),
    sprintf(
      "- Highest-complexity function: %s (%s)",
      highest_complexity_row$fn_name[[1L]],
      highest_complexity_row$best_class[[1L]]
    ),
    sprintf(
      "- Primary bottleneck candidate: %s (%s, %.1f%% impact)",
      primary_bottleneck_row$fn_name[[1L]],
      primary_bottleneck_row$best_class[[1L]],
      100 * primary_bottleneck_row$relative_impact[[1L]]
    ),
    sprintf("- Runtime projection sample n values: %s", sample_n_label),
    "",
    "## Function-Level Performance Matrix",
    "",
    .build_plaintext_matrix(function_matrix_dt),
    "",
    "## Bottleneck Candidates",
    "",
    bottleneck_lines,
    "",
    "## Complexity Distribution (ASCII)",
    "",
    .build_plaintext_matrix(complexity_dist_matrix_dt),
    "",
    "## Runtime Share Distribution (ASCII)",
    "",
    .build_plaintext_matrix(runtime_dist_matrix_dt),
    "",
    "## Runtime Projection Grid",
    "",
    .build_plaintext_matrix(projection_matrix_dt)
  )
}

#' @title Build project-level summary markdown from metrics
#' @description Build concise cross-pipeline markdown summary from precomputed
#'   reporting metrics.
#' @param metrics A list returned by prepare_reporting_metrics().
#' @param stage_ids Character vector of stage identifiers to summarize.
#' @return Character vector of markdown lines.
#' @keywords internal
#' @noRd
.build_general_project_performance_markdown_from_metrics <- function(
  metrics,
  stage_ids
) {
  summary_dt <- data.table::data.table(stage = stage_ids)
  summary_dt <- metrics$stage_summary[summary_dt, on = "stage"]

  summary_dt[is.na(function_count), function_count := 0L]
  summary_dt[is.na(max_complexity), max_complexity := "unknown"]
  summary_dt[is.na(stage_runtime_proportion), stage_runtime_proportion := 0]

  top_complexity_dt <- metrics$function_metrics[
    order(stage, -complexity_rank, -relative_impact, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "best_class")
  ]
  data.table::setnames(
    top_complexity_dt,
    old = c("fn_name", "best_class"),
    new = c("highest_complexity_fn", "highest_complexity_class")
  )

  bottleneck_text_dt <- metrics$function_metrics[
    order(stage, -relative_impact, -complexity_rank, fn_name),
    .(
      identified_bottlenecks = paste(
        head(
          sprintf(
            "%s (%s, %.1f%%)",
            fn_name,
            best_class,
            100 * relative_impact
          ),
          2L
        ),
        collapse = "; "
      )
    ),
    by = stage
  ]

  summary_dt <- top_complexity_dt[summary_dt, on = "stage"]
  summary_dt <- bottleneck_text_dt[summary_dt, on = "stage"]

  summary_dt[is.na(highest_complexity_fn), highest_complexity_fn := "N/A"]
  summary_dt[
    is.na(highest_complexity_class),
    highest_complexity_class := "unknown"
  ]
  summary_dt[
    is.na(identified_bottlenecks) | !nzchar(identified_bottlenecks),
    identified_bottlenecks := "None identified"
  ]

  summary_dt[,
    runtime_share_label := sprintf("%.1f%%", 100 * stage_runtime_proportion)
  ]

  summary_dt[, pipeline := .resolve_stage_report_label(stage)]
  summary_matrix_dt <- summary_dt[, .(
    Pipeline = pipeline,
    `Total functions` = as.character(function_count),
    `Highest-complexity function` = sprintf(
      "%s (%s)",
      highest_complexity_fn,
      highest_complexity_class
    ),
    `Dominant complexity` = max_complexity,
    `Identified bottlenecks` = identified_bottlenecks,
    `Runtime share` = runtime_share_label
  )]

  runtime_bottleneck_label <- if (!is.na(metrics$stage_bottleneck_runtime)) {
    .resolve_stage_report_label(metrics$stage_bottleneck_runtime)
  } else {
    "N/A"
  }

  complexity_bottleneck_label <- if (
    !is.na(metrics$stage_bottleneck_complexity)
  ) {
    .resolve_stage_report_label(metrics$stage_bottleneck_complexity)
  } else {
    "N/A"
  }

  c(
    "# General Project Performance",
    "",
    sprintf(
      "- Analysis timestamp (UTC): %s",
      format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    ),
    sprintf("- Overall pipeline class: %s", metrics$overall_class),
    sprintf("- Runtime bottleneck pipeline: %s", runtime_bottleneck_label),
    sprintf(
      "- Complexity bottleneck pipeline: %s",
      complexity_bottleneck_label
    ),
    sprintf(
      "- Primary function bottleneck: %s (%s)",
      metrics$function_bottleneck,
      metrics$function_bottleneck_stage
    ),
    "",
    "## Pipeline Summary",
    "",
    .build_plaintext_matrix(summary_matrix_dt)
  )
}

#' @title Build pipeline markdown bundle
#' @description Build markdown content for all per-pipeline files plus the
#'   project-level summary file.
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @return A named list of character vectors where each name is a markdown
#'   filename and each value is the file content line vector.
build_pipeline_markdown_bundle <- function(results) {
  metrics <- prepare_reporting_metrics(results)

  stage_ids <- .order_stage_ids_for_reports(
    c(
      names(get_perf_pipeline_report_file_map()),
      metrics$stage_summary$stage,
      metrics$function_metrics$stage
    )
  )

  pipeline_reports <- lapply(stage_ids, function(stage_id) {
    .build_pipeline_markdown_from_metrics(metrics, stage_id)
  })
  names(pipeline_reports) <- vapply(
    stage_ids,
    .resolve_stage_report_filename,
    character(1)
  )

  pipeline_reports[[get_perf_general_summary_report_filename()]] <-
    .build_general_project_performance_markdown_from_metrics(metrics, stage_ids)

  pipeline_reports
}

#' @title Export analysis Markdown
#' @description Write one markdown report per pipeline stage plus a project-level
#'   summary markdown report.
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @param output_path A character scalar output directory. If a file path ending
#'   in .md is supplied, its parent directory is used.
#' @return An invisible named character vector of written markdown paths.
export_analysis_markdown <- function(results, output_path) {
  output_dir <- if (grepl("\\.md$", output_path, ignore.case = TRUE)) {
    dirname(output_path)
  } else {
    output_path
  }

  if (!nzchar(output_dir)) {
    output_dir <- "."
  }

  markdown_bundle <- build_pipeline_markdown_bundle(results)

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  markdown_paths <- file.path(output_dir, names(markdown_bundle))
  names(markdown_paths) <- names(markdown_bundle)

  for (i in seq_along(markdown_bundle)) {
    writeLines(markdown_bundle[[i]], con = markdown_paths[[i]])
  }

  message(sprintf("  Markdown reports written to: %s", output_dir))
  return(invisible(markdown_paths))
}

#' @title Export results Markdown alias
#' @description Alias for export_analysis_markdown().
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @param output_path A character scalar output file path.
#' @return An invisible character scalar output path.
export_results_markdown <- function(results, output_path) {
  export_analysis_markdown(results, output_path)
}

# ── 8c. object persistence ────────────────────────────────────────────────────

#' @title Persist analysis object
#' @description Save the full analysis object using readr::write_rds() when
#'   available (fast, no compression), and fall back to base saveRDS() when
#'   readr is unavailable.
#' @param analysis_object An arbitrary R object to persist.
#' @param qs_path A character scalar destination path. If it ends in .qs, the
#'   output is written as .rds.
#' @return An invisible character scalar final path written.
persist_analysis <- function(analysis_object, qs_path) {
  out_path <- if (grepl("\\.qs$", qs_path, ignore.case = TRUE)) {
    sub("\\.qs$", ".rds", qs_path, ignore.case = TRUE)
  } else {
    qs_path
  }
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

  if (requireNamespace("readr", quietly = TRUE)) {
    # Fast-write profile: no compression favors write speed over file size.
    readr::write_rds(analysis_object, out_path, compress = "none")
    message(sprintf("  Analysis object saved to: %s", out_path))
  } else {
    warning(
      "package 'readr' not available; falling back to saveRDS(compress = FALSE). ",
      "Install readr for consistent fast-write behavior.\n",
      "Object saved to: ",
      out_path
    )
    saveRDS(analysis_object, out_path, compress = FALSE)
    message(sprintf("  Analysis object saved to: %s", out_path))
  }

  return(invisible(out_path))
}
