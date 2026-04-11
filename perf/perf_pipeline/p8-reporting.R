#' @title Reporting and persistence module
#' @description Console reporting, Markdown export, and analysis persistence
#'   helpers for the performance framework.
#' @keywords internal
#' @noRd
NULL

# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------

# Internal reporting constants used by markdown metric construction.
.reporting_runtime_sample_n <- as.integer(c(1000L, 10000L, 50000L))
.reporting_expensive_rank_threshold <- .complexity_order[["O(n^2)"]]
.reporting_high_impact_threshold <- 0.35
.reporting_plaintext_max_col_width <- 52L
.reporting_low_confidence_r2_threshold <- 0.90
.reporting_high_volatility_cv_threshold <- 0.20
.reporting_high_volatility_p99_ratio_threshold <- 1.75
.reporting_high_growth_multiplier_threshold <- 2.00
.reporting_critical_bottleneck_score_threshold <- 0.70
.reporting_bottleneck_score_weights <- c(
  complexity = 0.30,
  impact = 0.35,
  confidence = 0.15,
  volatility = 0.10,
  growth = 0.10
)

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

#' @title Clamp numeric values
#' @description Bound numeric values to the closed interval [lower, upper].
#' @param x Numeric vector.
#' @param lower Numeric scalar lower bound.
#' @param upper Numeric scalar upper bound.
#' @return Numeric vector.
#' @keywords internal
#' @noRd
.clamp_num <- function(x, lower = 0, upper = 1) {
  pmax(lower, pmin(upper, x))
}

#' @title Safe ratio
#' @description Compute deterministic ratios with explicit NA for invalid
#'   denominators.
#' @param numerator Numeric vector.
#' @param denominator Numeric vector.
#' @return Numeric vector.
#' @keywords internal
#' @noRd
.safe_ratio <- function(numerator, denominator) {
  out <- rep(NA_real_, length(numerator))
  valid <- !is.na(numerator) & !is.na(denominator) &
    is.finite(numerator) & is.finite(denominator) & denominator > 0
  if (any(valid)) {
    out[valid] <- numerator[valid] / denominator[valid]
  }
  out
}

#' @title Complexity basis value
#' @description Return model basis values for complexity classes and input size.
#' @param class_label Character scalar complexity class.
#' @param n_value Numeric vector input sizes.
#' @return Numeric vector transformed input sizes.
#' @keywords internal
#' @noRd
.complexity_basis_value <- function(class_label, n_value) {
  n_value <- as.numeric(n_value)
  safe_n <- pmax(1, n_value)

  switch(
    class_label,
    "O(1)" = rep(1, length(safe_n)),
    "O(log n)" = log(safe_n),
    "O(n)" = safe_n,
    "O(n log n)" = safe_n * log(safe_n),
    "O(n^2)" = safe_n^2,
    "O(n^3)" = safe_n^3,
    safe_n
  )
}

#' @title Confidence label from adjusted R-squared
#' @description Map adjusted R-squared values to deterministic confidence bins.
#' @param r_squared Numeric scalar adjusted R-squared.
#' @return Character scalar confidence label.
#' @keywords internal
#' @noRd
.confidence_label_from_r2 <- function(r_squared) {
  if (is.na(r_squared) || !is.finite(r_squared)) {
    return("unknown")
  }

  if (r_squared >= 0.98) {
    return("very_high")
  }
  if (r_squared >= 0.95) {
    return("high")
  }
  if (r_squared >= .reporting_low_confidence_r2_threshold) {
    return("moderate")
  }
  if (r_squared >= 0.75) {
    return("low")
  }

  "very_low"
}

#' @title Build deterministic flag summary
#' @description Collapse boolean diagnostic flags into a stable pipe-separated
#'   label string.
#' @param high_complexity Logical scalar.
#' @param high_impact Logical scalar.
#' @param low_confidence Logical scalar.
#' @param high_volatility Logical scalar.
#' @param critical_bottleneck Logical scalar.
#' @return Character scalar.
#' @keywords internal
#' @noRd
.compose_flag_summary <- function(
  high_complexity,
  high_impact,
  low_confidence,
  high_volatility,
  critical_bottleneck
) {
  labels <- c(
    if (isTRUE(high_complexity)) "high_complexity" else NULL,
    if (isTRUE(high_impact)) "high_impact" else NULL,
    if (isTRUE(low_confidence)) "low_confidence" else NULL,
    if (isTRUE(high_volatility)) "high_volatility" else NULL,
    if (isTRUE(critical_bottleneck)) "critical_bottleneck" else NULL
  )

  if (length(labels) == 0L) {
    return("ok")
  }

  paste(labels, collapse = "|")
}

#' @title Infer likely slowdown drivers
#' @description Build a deterministic explanatory sentence for likely slowdown
#'   causes using benchmark description and measured diagnostics.
#' @param description Character scalar benchmark description.
#' @param best_class Character scalar fitted complexity class.
#' @param relative_impact Numeric scalar runtime impact in stage.
#' @param growth_multiplier_max Numeric scalar maximum growth multiplier.
#' @param high_complexity Logical scalar.
#' @param high_impact Logical scalar.
#' @param low_confidence Logical scalar.
#' @param high_volatility Logical scalar.
#' @return Character scalar explanation.
#' @keywords internal
#' @noRd
.infer_slowdown_driver <- function(
  description,
  best_class,
  relative_impact,
  growth_multiplier_max,
  high_complexity,
  high_impact,
  low_confidence,
  high_volatility
) {
  desc <- tolower(as.character(description))
  causes <- character(0)

  if (grepl("melt|reshape|long format|wide table", desc, perl = TRUE)) {
    causes <- c(causes, "reshape pressure likely amplifies memory movement")
  }
  if (grepl("sort|reorder|sorted", desc, perl = TRUE)) {
    causes <- c(causes, "sorting/reordering work adds comparison overhead")
  }
  if (grepl("group|aggregate|duplicate", desc, perl = TRUE)) {
    causes <- c(causes, "grouping across repeated keys increases hash/scan cost")
  }
  if (grepl("normalize|string|ascii|coerce", desc, perl = TRUE)) {
    causes <- c(causes, "string normalization/coercion is CPU and allocation heavy")
  }
  if (grepl("copy|deep-copy", desc, perl = TRUE)) {
    causes <- c(causes, "table copying adds avoidable memory-bandwidth load")
  }
  if (grepl("join|lookup|key", desc, perl = TRUE)) {
    causes <- c(causes, "lookup/join paths may be index-sensitive")
  }

  if (isTRUE(high_complexity)) {
    causes <- c(causes, sprintf("super-linear class (%s) raises asymptotic risk", best_class))
  }
  if (isTRUE(high_impact)) {
    causes <- c(
      causes,
      sprintf("runtime concentration is high (%.1f%% of stage runtime)", 100 * relative_impact)
    )
  }
  if (!is.na(growth_multiplier_max) && is.finite(growth_multiplier_max) &&
    growth_multiplier_max >= .reporting_high_growth_multiplier_threshold) {
    causes <- c(causes, sprintf("runtime jumps sharply between sizes (max %.2fx)", growth_multiplier_max))
  }
  if (isTRUE(high_volatility)) {
    causes <- c(causes, "high run-to-run spread suggests unstable execution path")
  }
  if (isTRUE(low_confidence)) {
    causes <- c(causes, "model fit confidence is low; scaling class is uncertain")
  }

  causes <- unique(causes)
  if (length(causes) == 0L) {
    return("no dominant slowdown signal detected from current metrics")
  }

  paste(causes, collapse = "; ")
}

#' @title Build optimization reason string
#' @description Compose deterministic optimization reasoning text from
#'   diagnostic flags.
#' @param high_complexity Logical scalar.
#' @param high_impact Logical scalar.
#' @param low_confidence Logical scalar.
#' @param high_volatility Logical scalar.
#' @param growth_multiplier_max Numeric scalar growth multiplier.
#' @return Character scalar reason text.
#' @keywords internal
#' @noRd
.build_priority_reason <- function(
  high_complexity,
  high_impact,
  low_confidence,
  high_volatility,
  growth_multiplier_max
) {
  parts <- c(
    if (isTRUE(high_impact)) "large runtime share" else NULL,
    if (isTRUE(high_complexity)) "super-linear growth risk" else NULL,
    if (isTRUE(high_volatility)) "unstable repeated timings" else NULL,
    if (isTRUE(low_confidence)) "low model confidence" else NULL,
    if (!is.na(growth_multiplier_max) &&
      growth_multiplier_max >= .reporting_high_growth_multiplier_threshold) {
      sprintf("sharp growth jump (max %.2fx)", growth_multiplier_max)
    } else {
      NULL
    }
  )

  if (length(parts) == 0L) {
    return("monitor; no acute risk signal")
  }

  paste(unique(parts), collapse = "; ")
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
  raw_dt <- if ("raw" %in% names(results)) {
    data.table::as.data.table(results$raw)
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
      bottleneck_flag = character(0),
      mean_r_squared = numeric(0),
      median_r_squared = numeric(0),
      low_confidence_function_count = integer(0),
      high_volatility_function_count = integer(0),
      critical_bottleneck_count = integer(0),
      stage_bottleneck_score_max = numeric(0),
      stage_bottleneck_score_mean = numeric(0),
      runtime_concentration_hhi = numeric(0),
      top_function_impact = numeric(0),
      confidence_low_share = numeric(0),
      volatility_share = numeric(0),
      critical_share = numeric(0),
      stage_risk_score = numeric(0)
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
      function_bottleneck_stage = NA_character_,
      stage_bottleneck_matrix = data.table::data.table(),
      global_bottlenecks = data.table::data.table(),
      stage_priority_queue = data.table::data.table()
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

  complexity_dt[, `:=`(
    fn_name = as.character(fn_name),
    stage = as.character(stage),
    description = as.character(description),
    best_class = as.character(best_class),
    r_squared = as.numeric(r_squared),
    slope_per_n = as.numeric(slope_per_n)
  )]
  complexity_dt[is.na(description) | !nzchar(description), description := "N/A"]

  if (!"intercept" %in% names(complexity_dt)) {
    complexity_dt[, intercept := NA_real_]
  } else {
    complexity_dt[, intercept := as.numeric(intercept)]
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
      .(stage_max_rank = {
        valid <- complexity_rank[!is.na(complexity_rank) & is.finite(complexity_rank)]
        if (length(valid) > 0L) max(valid) else .complexity_order[["unknown"]]
      }),
      by = stage
    ]
    complexity_dt <- stage_max[complexity_dt, on = "stage"]
    complexity_dt[, dominant_in_stage := (!is.na(complexity_rank) & complexity_rank == stage_max_rank)]
    complexity_dt[, stage_max_rank := NULL]
  }

  has_runtime_summary <- nrow(summary_dt) > 0L &&
    all(c("fn_name", "stage", "n", "median_s") %in% names(summary_dt))
  has_raw_timing <- nrow(raw_dt) > 0L &&
    all(c("fn_name", "stage", "n", "elapsed_s") %in% names(raw_dt))

  if (!has_runtime_summary && has_raw_timing) {
    summary_dt <- raw_dt[
      !is.na(elapsed_s) & is.finite(elapsed_s),
      .(
        median_s = stats::median(elapsed_s),
        mean_s = mean(elapsed_s),
        sd_s = stats::sd(elapsed_s),
        min_s = min(elapsed_s),
        max_s = max(elapsed_s),
        iqr_s = stats::IQR(elapsed_s, na.rm = TRUE),
        p95_s = as.numeric(stats::quantile(
          elapsed_s,
          probs = 0.95,
          type = 7,
          names = FALSE,
          na.rm = TRUE
        )),
        p99_s = as.numeric(stats::quantile(
          elapsed_s,
          probs = 0.99,
          type = 7,
          names = FALSE,
          na.rm = TRUE
        )),
        cv_s = ifelse(mean(elapsed_s) > 0, stats::sd(elapsed_s) / mean(elapsed_s), NA_real_),
        n_reps = .N
      ),
      by = .(fn_name, stage, n)
    ]
    has_runtime_summary <- TRUE
  }

  if (has_runtime_summary) {
    default_summary_cols <- c(
      "sd_s",
      "iqr_s",
      "p95_s",
      "p99_s",
      "cv_s",
      "n_reps"
    )
    for (col_i in default_summary_cols) {
      if (!col_i %in% names(summary_dt)) {
        summary_dt[, (col_i) := NA_real_]
      }
    }

    summary_dt[, `:=`(
      fn_name = as.character(fn_name),
      stage = as.character(stage),
      n = as.integer(n),
      median_s = as.numeric(median_s),
      sd_s = as.numeric(sd_s),
      iqr_s = as.numeric(iqr_s),
      p95_s = as.numeric(p95_s),
      p99_s = as.numeric(p99_s),
      cv_s = as.numeric(cv_s)
    )]

    fn_runtime_dt <- summary_dt[order(n), {
      ord <- order(n)
      n_sorted <- as.integer(n[ord])
      med_sorted <- as.numeric(median_s[ord])
      sd_sorted <- as.numeric(sd_s[ord])
      iqr_sorted <- as.numeric(iqr_s[ord])
      p95_sorted <- as.numeric(p95_s[ord])
      p99_sorted <- as.numeric(p99_s[ord])
      cv_sorted <- as.numeric(cv_s[ord])

      idx_min <- if (length(n_sorted) > 0L) which.min(n_sorted) else NA_integer_
      idx_max <- if (length(n_sorted) > 0L) which.max(n_sorted) else NA_integer_

      growth_vals <- numeric(0)
      growth_labels <- character(0)
      if (length(n_sorted) >= 2L) {
        for (j in 2:length(n_sorted)) {
          prev <- med_sorted[[j - 1L]]
          curr <- med_sorted[[j]]
          if (is.finite(prev) && prev > 0 && is.finite(curr) && curr >= 0) {
            mult <- curr / prev
            growth_vals <- c(growth_vals, mult)
            growth_labels <- c(
              growth_labels,
              sprintf("%d->%d: %.2fx", n_sorted[[j - 1L]], n_sorted[[j]], mult)
            )
          }
        }
      }

      valid_growth <- growth_vals[is.finite(growth_vals) & growth_vals > 0]
      valid_med <- med_sorted[is.finite(med_sorted)]

      .(
        observed_runtime_s = if (length(valid_med) > 0L) sum(valid_med) else 0,
        runtime_at_min_n_s = if (!is.na(idx_min)) med_sorted[[idx_min]] else NA_real_,
        runtime_at_max_n_s = if (!is.na(idx_max)) med_sorted[[idx_max]] else NA_real_,
        n_min = if (!is.na(idx_min)) n_sorted[[idx_min]] else NA_integer_,
        n_max = if (!is.na(idx_max)) n_sorted[[idx_max]] else NA_integer_,
        volatility_sd_s = if (!is.na(idx_max)) sd_sorted[[idx_max]] else NA_real_,
        volatility_iqr_s = if (!is.na(idx_max)) iqr_sorted[[idx_max]] else NA_real_,
        volatility_p95_s = if (!is.na(idx_max)) p95_sorted[[idx_max]] else NA_real_,
        volatility_p99_s = if (!is.na(idx_max)) p99_sorted[[idx_max]] else NA_real_,
        volatility_cv = if (!is.na(idx_max)) cv_sorted[[idx_max]] else NA_real_,
        growth_multiplier_max = if (length(valid_growth) > 0L) max(valid_growth) else NA_real_,
        growth_multiplier_last = if (length(valid_growth) > 0L) valid_growth[[length(valid_growth)]] else NA_real_,
        growth_multiplier_geom = if (length(valid_growth) > 0L) exp(mean(log(valid_growth))) else NA_real_,
        growth_profile = if (length(growth_labels) > 0L) {
          paste(growth_labels, collapse = "; ")
        } else {
          "N/A"
        }
      )
    }, by = .(fn_name, stage)]

    n_candidates <- sort(unique(as.integer(summary_dt$n[
      is.finite(summary_dt$n) & summary_dt$n > 0
    ])))
  } else {
    fn_runtime_dt <- complexity_dt[, .(fn_name, stage)]
    fn_runtime_dt[, `:=`(
      observed_runtime_s = 0,
      runtime_at_min_n_s = NA_real_,
      runtime_at_max_n_s = NA_real_,
      n_min = NA_integer_,
      n_max = NA_integer_,
      volatility_sd_s = NA_real_,
      volatility_iqr_s = NA_real_,
      volatility_p95_s = NA_real_,
      volatility_p99_s = NA_real_,
      volatility_cv = NA_real_,
      growth_multiplier_max = NA_real_,
      growth_multiplier_last = NA_real_,
      growth_multiplier_geom = NA_real_,
      growth_profile = "N/A"
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

  fn_metrics <- merge(
    complexity_dt,
    fn_runtime_dt,
    by = c("fn_name", "stage"),
    all.x = TRUE,
    sort = FALSE
  )
  fn_metrics[is.na(observed_runtime_s), observed_runtime_s := 0]
  fn_metrics[is.na(runtime_at_min_n_s), runtime_at_min_n_s := observed_runtime_s]
  fn_metrics[
    is.na(runtime_at_max_n_s),
    runtime_at_max_n_s := observed_runtime_s
  ]
  fn_metrics[is.na(growth_profile) | !nzchar(growth_profile), growth_profile := "N/A"]

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
    high_complexity_flag := complexity_rank >= .reporting_expensive_rank_threshold
  ]
  fn_metrics[is.na(high_complexity_flag), high_complexity_flag := FALSE]
  fn_metrics[,
    high_impact_flag := relative_impact >= .reporting_high_impact_threshold
  ]
  fn_metrics[is.na(high_impact_flag), high_impact_flag := FALSE]
  fn_metrics[,
    low_confidence_flag := is.na(r_squared) |
      !is.finite(r_squared) |
      r_squared < .reporting_low_confidence_r2_threshold
  ]
  fn_metrics[,
    confidence_label := vapply(
      r_squared,
      .confidence_label_from_r2,
      character(1)
    )
  ]
  fn_metrics[,
    volatility_ratio_p99_to_p50 := .safe_ratio(
      volatility_p99_s,
      runtime_at_max_n_s
    )
  ]
  fn_metrics[,
    high_volatility_flag := (
      !is.na(volatility_cv) &
        is.finite(volatility_cv) &
        volatility_cv >= .reporting_high_volatility_cv_threshold
    ) |
      (
        !is.na(volatility_ratio_p99_to_p50) &
          is.finite(volatility_ratio_p99_to_p50) &
          volatility_ratio_p99_to_p50 >= .reporting_high_volatility_p99_ratio_threshold
      )
  ]
  fn_metrics[,
    high_growth_flag := !is.na(growth_multiplier_max) &
      is.finite(growth_multiplier_max) &
      growth_multiplier_max >= .reporting_high_growth_multiplier_threshold
  ]

  max_rank_ref <- .complexity_order[["O(n^3)"]]
  fn_metrics[,
    complexity_risk_component := data.table::fifelse(
      is.na(complexity_rank),
      0.5,
      data.table::fifelse(
        complexity_rank >= .complexity_order[["unknown"]],
        1,
        .clamp_num((complexity_rank - 1) / max(1, max_rank_ref - 1))
      )
    )
  ]
  fn_metrics[,
    impact_risk_component := .clamp_num(relative_impact)
  ]
  fn_metrics[,
    confidence_risk_component := data.table::fifelse(
      is.na(r_squared) | !is.finite(r_squared),
      1,
      .clamp_num(1 - pmax(r_squared, 0))
    )
  ]
  fn_metrics[,
    volatility_cv_component := .clamp_num(
      .safe_ratio(volatility_cv, .reporting_high_volatility_cv_threshold)
    )
  ]
  fn_metrics[is.na(volatility_cv_component), volatility_cv_component := 0]
  fn_metrics[,
    volatility_p99_component := .clamp_num(
      .safe_ratio(
        volatility_ratio_p99_to_p50 - 1,
        .reporting_high_volatility_p99_ratio_threshold - 1
      )
    )
  ]
  fn_metrics[is.na(volatility_p99_component), volatility_p99_component := 0]
  fn_metrics[,
    volatility_risk_component := pmax(
      volatility_cv_component,
      volatility_p99_component
    )
  ]
  fn_metrics[,
    growth_risk_component := .clamp_num(
      .safe_ratio(
        growth_multiplier_max - 1,
        .reporting_high_growth_multiplier_threshold - 1
      )
    )
  ]
  fn_metrics[is.na(growth_risk_component), growth_risk_component := 0]

  w <- .reporting_bottleneck_score_weights
  fn_metrics[,
    bottleneck_score := .clamp_num(
      w[["complexity"]] * complexity_risk_component +
        w[["impact"]] * impact_risk_component +
        w[["confidence"]] * confidence_risk_component +
        w[["volatility"]] * volatility_risk_component +
        w[["growth"]] * growth_risk_component
    )
  ]
  fn_metrics[,
    critical_bottleneck_flag :=
      (high_complexity_flag & high_impact_flag) |
      bottleneck_score >= .reporting_critical_bottleneck_score_threshold
  ]
  fn_metrics[,
    scaling_flag := data.table::fifelse(
      critical_bottleneck_flag,
      "critical",
      data.table::fifelse(
        high_complexity_flag,
        "high_complexity",
        data.table::fifelse(
          high_impact_flag,
          "high_impact",
          data.table::fifelse(
            low_confidence_flag | high_volatility_flag,
            "watch",
            "ok"
          )
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
          data.table::fifelse(
            scaling_flag == "watch",
            "?",
            "."
          )
        )
      )
    )
  ]
  fn_metrics[,
    diagnostic_flags := mapply(
      .compose_flag_summary,
      high_complexity_flag,
      high_impact_flag,
      low_confidence_flag,
      high_volatility_flag,
      critical_bottleneck_flag,
      USE.NAMES = FALSE
    )
  ]
  fn_metrics[,
    slowdown_drivers := mapply(
      .infer_slowdown_driver,
      description,
      best_class,
      relative_impact,
      growth_multiplier_max,
      high_complexity_flag,
      high_impact_flag,
      low_confidence_flag,
      high_volatility_flag,
      USE.NAMES = FALSE
    )
  ]
  fn_metrics[,
    priority_tier := data.table::fifelse(
      critical_bottleneck_flag | bottleneck_score >= 0.75,
      "P0",
      data.table::fifelse(
        bottleneck_score >= 0.55,
        "P1",
        "P2"
      )
    )
  ]
  fn_metrics[,
    optimization_reason := mapply(
      .build_priority_reason,
      high_complexity_flag,
      high_impact_flag,
      low_confidence_flag,
      high_volatility_flag,
      growth_multiplier_max,
      USE.NAMES = FALSE
    )
  ]
  fn_metrics[,
    expected_impact_label := sprintf(
      "up to %.1f%% stage runtime",
      100 * relative_impact
    )
  ]

  runtime_projection_dt <- fn_metrics[,
    .(n_sample = sample_n_values),
    by = .(fn_name, stage, best_class, slope_per_n, intercept)
  ]
  runtime_projection_dt[,
    basis_value := mapply(
      function(class_i, n_i) .complexity_basis_value(class_i, n_i),
      best_class,
      n_sample,
      USE.NAMES = FALSE
    )
  ]
  runtime_projection_dt[,
    estimated_runtime_s := {
      intercept_i <- data.table::fifelse(
        is.na(intercept) | !is.finite(intercept),
        0,
        intercept
      )
      est <- intercept_i + slope_per_n * basis_value
      data.table::fifelse(
        is.finite(est) & est >= 0,
        est,
        NA_real_
      )
    }
  ]
  runtime_projection_dt[, c("basis_value", "intercept") := NULL]

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

  fn_metrics[order(stage, -bottleneck_score, -relative_impact, -complexity_rank, fn_name),
    bottleneck_rank_in_stage := seq_len(.N),
    by = stage
  ]
  fn_metrics[order(-bottleneck_score, -relative_impact, -complexity_rank, stage, fn_name),
    global_bottleneck_rank := seq_len(.N)
  ]

  stage_summary_dt <- fn_metrics[,
    {
      valid_rank <- complexity_rank[!is.na(complexity_rank) & is.finite(complexity_rank)]
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

      valid_r2 <- r_squared[!is.na(r_squared) & is.finite(r_squared)]
      valid_scores <- bottleneck_score[!is.na(bottleneck_score) & is.finite(bottleneck_score)]
      valid_impacts <- relative_impact[!is.na(relative_impact) & is.finite(relative_impact)]

      .(
        function_count = .N,
        max_complexity_rank = as.integer(max_rank),
        max_complexity = max_complexity,
        expensive_function_count = as.integer(sum(high_complexity_flag, na.rm = TRUE)),
        stage_runtime_total_s = sum(observed_runtime_s, na.rm = TRUE),
        mean_r_squared = if (length(valid_r2) > 0L) mean(valid_r2) else NA_real_,
        median_r_squared = if (length(valid_r2) > 0L) stats::median(valid_r2) else NA_real_,
        low_confidence_function_count = as.integer(sum(low_confidence_flag, na.rm = TRUE)),
        high_volatility_function_count = as.integer(sum(high_volatility_flag, na.rm = TRUE)),
        critical_bottleneck_count = as.integer(sum(critical_bottleneck_flag, na.rm = TRUE)),
        stage_bottleneck_score_max = if (length(valid_scores) > 0L) max(valid_scores) else NA_real_,
        stage_bottleneck_score_mean = if (length(valid_scores) > 0L) mean(valid_scores) else NA_real_,
        runtime_concentration_hhi = sum(relative_impact^2, na.rm = TRUE),
        top_function_impact = if (length(valid_impacts) > 0L) max(valid_impacts) else 0
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
  stage_summary_dt[,
    confidence_low_share := data.table::fifelse(
      function_count > 0,
      low_confidence_function_count / function_count,
      0
    )
  ]
  stage_summary_dt[,
    volatility_share := data.table::fifelse(
      function_count > 0,
      high_volatility_function_count / function_count,
      0
    )
  ]
  stage_summary_dt[,
    critical_share := data.table::fifelse(
      function_count > 0,
      critical_bottleneck_count / function_count,
      0
    )
  ]
  stage_summary_dt[,
    stage_complexity_component := data.table::fifelse(
      is.na(max_complexity_rank),
      0.5,
      data.table::fifelse(
        max_complexity_rank >= .complexity_order[["unknown"]],
        1,
        .clamp_num((max_complexity_rank - 1) / max(1, max_rank_ref - 1))
      )
    )
  ]
  stage_summary_dt[,
    stage_risk_score := .clamp_num(
      0.40 * stage_runtime_proportion +
        0.25 * stage_complexity_component +
        0.15 * confidence_low_share +
        0.10 * volatility_share +
        0.10 * critical_share
    )
  ]
  stage_summary_dt[, stage_complexity_component := NULL]

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
  risk_max <- if (nrow(stage_summary_dt) > 0L) {
    max(stage_summary_dt$stage_risk_score, na.rm = TRUE)
  } else {
    NA_real_
  }

  stage_summary_dt[,
    bottleneck_flag := data.table::fifelse(
      abs(stage_runtime_proportion - runtime_max) < .Machine$double.eps^0.5 |
        max_complexity_rank == complexity_max |
        abs(stage_risk_score - risk_max) < .Machine$double.eps^0.5,
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
  overall_class <- if (length(dom_classes) > 0L) {
    dom_classes[[which.max(dom_ranks)]]
  } else {
    "unknown"
  }

  chart_complexity_dt <- fn_metrics[, .(
    stage,
    fn_name,
    best_class,
    complexity_rank,
    visual_indicator,
    bottleneck_score
  )][order(stage, -complexity_rank, fn_name)]

  chart_stage_runtime_dt <- stage_summary_dt[, .(
    stage,
    stage_runtime_total_s,
    stage_runtime_proportion,
    function_count,
    expensive_function_pct,
    max_complexity_rank,
    max_complexity,
    stage_risk_score,
    confidence_low_share,
    volatility_share
  )][order(-stage_runtime_proportion, -stage_risk_score, -max_complexity_rank, stage)]

  chart_slope_dt <- fn_metrics[, .(
    stage,
    fn_name,
    slope_per_n,
    relative_impact,
    scaling_flag,
    visual_indicator,
    bottleneck_score,
    confidence_label,
    volatility_cv
  )][order(-relative_impact, -bottleneck_score, -slope_per_n, stage, fn_name)]

  stage_bottleneck_matrix_dt <- fn_metrics[
    order(stage, -relative_impact, -bottleneck_score, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "relative_impact", "best_class")
  ]
  data.table::setnames(
    stage_bottleneck_matrix_dt,
    old = c("fn_name", "relative_impact", "best_class"),
    new = c("top_runtime_fn", "top_runtime_share", "top_runtime_class")
  )

  stage_top_complexity_dt <- fn_metrics[
    order(stage, -complexity_rank, -relative_impact, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "best_class")
  ]
  data.table::setnames(
    stage_top_complexity_dt,
    old = c("fn_name", "best_class"),
    new = c("top_complexity_fn", "top_complexity_class")
  )

  stage_top_score_dt <- fn_metrics[
    order(stage, -bottleneck_score, -relative_impact, -complexity_rank, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "bottleneck_score", "priority_tier")
  ]
  data.table::setnames(
    stage_top_score_dt,
    old = c("fn_name", "bottleneck_score", "priority_tier"),
    new = c("top_score_fn", "top_score", "top_priority_tier")
  )

  stage_bottleneck_matrix_dt <- stage_bottleneck_matrix_dt[
    stage_top_complexity_dt,
    on = "stage"
  ]
  stage_bottleneck_matrix_dt <- stage_bottleneck_matrix_dt[
    stage_top_score_dt,
    on = "stage"
  ]
  stage_bottleneck_matrix_dt <- stage_bottleneck_matrix_dt[
    stage_summary_dt[, .(
      stage,
      stage_runtime_proportion,
      stage_risk_score,
      low_confidence_function_count,
      high_volatility_function_count,
      critical_bottleneck_count
    )],
    on = "stage"
  ]
  stage_bottleneck_matrix_dt[,
    top_runtime_share_label := sprintf("%.1f%%", 100 * top_runtime_share)
  ]

  global_bottlenecks_dt <- fn_metrics[order(
    -bottleneck_score,
    -relative_impact,
    -complexity_rank,
    stage,
    fn_name
  ), .(
    global_rank = global_bottleneck_rank,
    stage,
    fn_name,
    best_class,
    bottleneck_score,
    relative_impact,
    confidence_label,
    volatility_cv,
    priority_tier,
    diagnostic_flags,
    slowdown_drivers
  )]
  global_bottlenecks_dt <- global_bottlenecks_dt[
    seq_len(min(nrow(global_bottlenecks_dt), 12L))
  ]

  stage_priority_queue_dt <- fn_metrics[order(
    stage,
    -bottleneck_score,
    -relative_impact,
    -complexity_rank,
    fn_name
  )][,
    head(.SD, 5L),
    by = stage,
    .SDcols = c(
      "fn_name",
      "best_class",
      "bottleneck_score",
      "relative_impact",
      "priority_tier",
      "optimization_reason",
      "expected_impact_label",
      "diagnostic_flags"
    )
  ]

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
    -bottleneck_score,
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
    function_bottleneck_stage = function_bottleneck_stage,
    stage_bottleneck_matrix = stage_bottleneck_matrix_dt,
    global_bottlenecks = global_bottlenecks_dt,
    stage_priority_queue = stage_priority_queue_dt
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
#' @param preset_name Optional preset label used to namespace markdown
#'   filenames.
#' @return A character scalar markdown filename.
#' @keywords internal
#' @noRd
.resolve_stage_report_filename <- function(stage_id, preset_name = NULL) {
  stage_id <- as.character(stage_id)
  file_map <- get_perf_pipeline_report_file_map(preset_name = preset_name)
  resolved <- unname(file_map[stage_id])

  missing_idx <- is.na(resolved) | !nzchar(resolved)
  if (any(missing_idx)) {
    safe_stage <- gsub("[^A-Za-z0-9_-]+", "_", stage_id[missing_idx])
    resolved[missing_idx] <- paste0("perf_", safe_stage, "_pipeline.md")
    if (!is.null(preset_name) && length(preset_name) > 0L) {
      resolved[missing_idx] <- vapply(
        resolved[missing_idx],
        apply_perf_preset_to_report_filename,
        character(1),
        preset_name = preset_name
      )
    }
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
#' @param preset_name A character scalar preset label used for report metadata.
#' @return Character vector of markdown lines.
#' @keywords internal
#' @noRd
.build_pipeline_markdown_from_metrics <- function(
  metrics,
  stage_id,
  preset_name = "custom"
) {
  stage_label <- .resolve_stage_report_label(stage_id)
  preset_label <- .sanitize_perf_preset_name(preset_name, default = "custom")

  stage_summary <- data.table::copy(metrics$stage_summary[stage == stage_id])
  fn_metrics <- data.table::copy(metrics$function_metrics[stage == stage_id])
  runtime_projection <- data.table::copy(metrics$runtime_projection[
    stage == stage_id
  ])
  stage_priority_queue <- if ("stage_priority_queue" %in% names(metrics)) {
    data.table::copy(metrics$stage_priority_queue[stage == stage_id])
  } else {
    data.table::data.table()
  }

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
      bottleneck_flag = "normal",
      mean_r_squared = NA_real_,
      median_r_squared = NA_real_,
      low_confidence_function_count = 0L,
      high_volatility_function_count = 0L,
      critical_bottleneck_count = 0L,
      stage_bottleneck_score_max = NA_real_,
      stage_bottleneck_score_mean = NA_real_,
      runtime_concentration_hhi = 0,
      top_function_impact = 0,
      confidence_low_share = 0,
      volatility_share = 0,
      critical_share = 0,
      stage_risk_score = 0
    )
  }

  if (nrow(fn_metrics) > 0L) {
    defaults <- list(
      bottleneck_score = NA_real_,
      confidence_label = "unknown",
      volatility_cv = NA_real_,
      volatility_ratio_p99_to_p50 = NA_real_,
      growth_multiplier_max = NA_real_,
      runtime_at_max_n_s = NA_real_,
      observed_runtime_s = NA_real_,
      diagnostic_flags = "ok",
      slowdown_drivers = "N/A",
      critical_bottleneck_flag = FALSE,
      low_confidence_flag = FALSE,
      high_volatility_flag = FALSE,
      high_complexity_flag = FALSE,
      high_impact_flag = FALSE,
      optimization_reason = "monitor; no acute risk signal",
      expected_impact_label = "up to 0.0% stage runtime",
      priority_tier = "P2"
    )
    for (nm in names(defaults)) {
      if (!nm %in% names(fn_metrics)) {
        fn_metrics[, (nm) := defaults[[nm]]]
      }
    }

    data.table::setorder(
      fn_metrics,
      -bottleneck_score,
      -relative_impact,
      -complexity_rank,
      fn_name
    )

    fn_metrics[,
      r_squared_label := .format_metric_value(r_squared, digits = 6L)
    ]
    fn_metrics[, slope_label := .format_metric_value(slope_per_n, digits = 6L)]
    fn_metrics[,
      relative_impact_label := sprintf("%.1f%%", 100 * relative_impact)
    ]
    fn_metrics[,
      observed_runtime_label := .format_metric_value(observed_runtime_s, digits = 5L)
    ]
    fn_metrics[,
      runtime_at_max_n_label := .format_metric_value(runtime_at_max_n_s, digits = 5L)
    ]
    fn_metrics[,
      bottleneck_score_label := .format_metric_value(bottleneck_score, digits = 4L)
    ]
    fn_metrics[,
      volatility_cv_label := .format_metric_value(volatility_cv, digits = 5L)
    ]
    fn_metrics[,
      p99_ratio_label := .format_metric_value(volatility_ratio_p99_to_p50, digits = 4L)
    ]
    fn_metrics[,
      growth_max_label := .format_metric_value(growth_multiplier_max, digits = 4L)
    ]
    fn_metrics[,
      confidence_label_fmt := gsub("_", " ", confidence_label, fixed = TRUE)
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
    fn_metrics[,
      bottleneck_label := data.table::fifelse(
        critical_bottleneck_flag,
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
      `Estimated runtime (sample n)` = estimated_runtime_summary,
      `Relative impact` = relative_impact_label,
      Indicator = indicator_label,
      Bottleneck = bottleneck_label,
      `Complexity rank` = as.character(complexity_rank),
      Confidence = confidence_label_fmt,
      `Observed runtime total (s)` = observed_runtime_label,
      `Runtime at max n (s)` = runtime_at_max_n_label,
      `Volatility cv` = volatility_cv_label,
      `p99/p50 ratio` = p99_ratio_label,
      `Growth max` = growth_max_label,
      `Composite score` = bottleneck_score_label,
      Flags = diagnostic_flags,
      `Dominant in stage` = dominant_stage_label,
      `Likely slowdown drivers` = slowdown_drivers
    )]

    highest_complexity_row <- fn_metrics[order(
      -complexity_rank,
      -bottleneck_score,
      -relative_impact,
      fn_name
    )][1L]
    primary_bottleneck_row <- fn_metrics[order(
      -bottleneck_score,
      -relative_impact,
      -complexity_rank,
      fn_name
    )][1L]
    runtime_driver_row <- fn_metrics[order(
      -relative_impact,
      -bottleneck_score,
      fn_name
    )][1L]

    complexity_dist <- fn_metrics[, .(
      function_count = .N,
      runtime_share = sum(relative_impact, na.rm = TRUE)
    ), by = best_class]
    complexity_dist[,
      complexity_rank := match(
        best_class,
        names(.complexity_order),
        nomatch = .complexity_order[["unknown"]]
      )
    ]
    complexity_dist <- complexity_dist[order(
      -runtime_share,
      -function_count,
      -complexity_rank,
      best_class
    )]
    complexity_dist[, share_label := sprintf("%.1f%%", 100 * runtime_share)]
    complexity_dist[, count_share := function_count / sum(function_count)]
    complexity_dist[, count_share_label := sprintf("%.1f%%", 100 * count_share)]
    complexity_dist[, share_bar := .build_ascii_bar(runtime_share, width = 24L)]

    complexity_dist_matrix_dt <- complexity_dist[, .(
      `Complexity class` = best_class,
      `Function count` = as.character(function_count),
      `Function share` = count_share_label,
      `Runtime share` = share_label,
      Distribution = share_bar
    )]

    runtime_dist <- fn_metrics[, .(fn_name, relative_impact, bottleneck_score)]
    data.table::setorder(runtime_dist, -relative_impact, -bottleneck_score, fn_name)
    runtime_dist[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]
    runtime_dist[, impact_bar := .build_ascii_bar(relative_impact, width = 24L)]
    runtime_dist[, score_label := .format_metric_value(bottleneck_score, digits = 4L)]

    runtime_dist_matrix_dt <- runtime_dist[, .(
      Function = fn_name,
      `Relative impact` = impact_label,
      `Composite score` = score_label,
      Distribution = impact_bar
    )]

    bottleneck_candidates <- fn_metrics[order(
      -bottleneck_score,
      -relative_impact,
      -complexity_rank,
      fn_name
    )][seq_len(min(5L, .N))]
    bottleneck_candidates[,
      impact_label := sprintf("%.1f%%", 100 * relative_impact)
    ]

    if (nrow(bottleneck_candidates) > 0L) {
      bottleneck_lines <- sprintf(
        "- %s: class %s, score %s, impact %s, flags %s.",
        .sanitize_plaintext_cell(bottleneck_candidates$fn_name),
        .sanitize_plaintext_cell(bottleneck_candidates$best_class),
        .format_metric_value(bottleneck_candidates$bottleneck_score, digits = 4L),
        bottleneck_candidates$impact_label,
        .sanitize_plaintext_cell(bottleneck_candidates$diagnostic_flags)
      )
    } else {
      bottleneck_lines <- "- None identified."
    }

    top_bottleneck_matrix_dt <- bottleneck_candidates[, .(
      Rank = as.character(seq_len(.N)),
      Function = fn_name,
      Complexity = best_class,
      `Composite score` = .format_metric_value(bottleneck_score, digits = 4L),
      `Stage impact` = sprintf("%.1f%%", 100 * relative_impact),
      `adj.R2` = .format_metric_value(r_squared, digits = 5L),
      Confidence = gsub("_", " ", confidence_label, fixed = TRUE),
      `Volatility cv` = .format_metric_value(volatility_cv, digits = 5L),
      `Growth max` = .format_metric_value(growth_multiplier_max, digits = 4L),
      Flags = diagnostic_flags,
      `Likely slowdown drivers` = slowdown_drivers
    )]

    if (nrow(stage_priority_queue) == 0L) {
      stage_priority_queue <- fn_metrics[,
        .(
          fn_name,
          best_class,
          bottleneck_score,
          relative_impact,
          priority_tier,
          optimization_reason,
          expected_impact_label,
          diagnostic_flags
        )
      ][order(-bottleneck_score, -relative_impact, -complexity_rank, fn_name)]
      stage_priority_queue <- stage_priority_queue[seq_len(min(5L, .N))]
    }

    priority_matrix_dt <- stage_priority_queue[, .(
      `Priority tier` = priority_tier,
      Function = fn_name,
      Complexity = best_class,
      `Composite score` = .format_metric_value(bottleneck_score, digits = 4L),
      `Stage impact` = sprintf("%.1f%%", 100 * relative_impact),
      Reason = optimization_reason,
      `Expected impact` = expected_impact_label,
      Flags = diagnostic_flags
    )]

    confidence_summary_matrix_dt <- data.table::data.table(
      Metric = c(
        "Mean adjusted R2",
        "Median adjusted R2",
        "Low-confidence functions",
        "Low-confidence share",
        "High-volatility functions",
        "High-volatility share",
        "Critical bottlenecks",
        "Runtime concentration (HHI)"
      ),
      Value = c(
        .format_metric_value(stage_summary$mean_r_squared[[1L]], digits = 5L),
        .format_metric_value(stage_summary$median_r_squared[[1L]], digits = 5L),
        as.character(stage_summary$low_confidence_function_count[[1L]]),
        sprintf("%.1f%%", 100 * stage_summary$confidence_low_share[[1L]]),
        as.character(stage_summary$high_volatility_function_count[[1L]]),
        sprintf("%.1f%%", 100 * stage_summary$volatility_share[[1L]]),
        as.character(stage_summary$critical_bottleneck_count[[1L]]),
        .format_metric_value(stage_summary$runtime_concentration_hhi[[1L]], digits = 5L)
      )
    )

    stage_kpi_matrix_dt <- data.table::data.table(
      KPI = c(
        "Total functions benchmarked",
        "Dominant complexity class",
        "Stage runtime total (s)",
        "Stage runtime share",
        "Expensive function share",
        "Stage risk score",
        "Top runtime driver",
        "Top composite bottleneck"
      ),
      Value = c(
        as.character(stage_summary$function_count[[1L]]),
        stage_summary$max_complexity[[1L]],
        .format_metric_value(stage_summary$stage_runtime_total_s[[1L]], digits = 5L),
        sprintf("%.1f%%", 100 * stage_summary$stage_runtime_proportion[[1L]]),
        sprintf("%.1f%%", stage_summary$expensive_function_pct[[1L]]),
        .format_metric_value(stage_summary$stage_risk_score[[1L]], digits = 4L),
        sprintf(
          "%s (%.1f%%)",
          runtime_driver_row$fn_name[[1L]],
          100 * runtime_driver_row$relative_impact[[1L]]
        ),
        sprintf(
          "%s (score %s)",
          primary_bottleneck_row$fn_name[[1L]],
          .format_metric_value(primary_bottleneck_row$bottleneck_score[[1L]], digits = 4L)
        )
      )
    )

    stage_narrative_lines <- c(
      sprintf(
        "- Runtime is dominated by %s, contributing %.1f%% of stage runtime.",
        .sanitize_plaintext_cell(runtime_driver_row$fn_name[[1L]]),
        100 * runtime_driver_row$relative_impact[[1L]]
      ),
      sprintf(
        "- Asymptotic risk is dominated by %s with class %s.",
        .sanitize_plaintext_cell(highest_complexity_row$fn_name[[1L]]),
        .sanitize_plaintext_cell(highest_complexity_row$best_class[[1L]])
      ),
      sprintf(
        "- Optimize first: %s because %s; expected impact %s.",
        .sanitize_plaintext_cell(primary_bottleneck_row$fn_name[[1L]]),
        .sanitize_plaintext_cell(primary_bottleneck_row$optimization_reason[[1L]]),
        .sanitize_plaintext_cell(primary_bottleneck_row$expected_impact_label[[1L]])
      )
    )
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
      Bottleneck = "no",
      `Complexity rank` = "7",
      Confidence = "unknown",
      `Observed runtime total (s)` = "N/A",
      `Runtime at max n (s)` = "N/A",
      `Volatility cv` = "N/A",
      `p99/p50 ratio` = "N/A",
      `Growth max` = "N/A",
      `Composite score` = "N/A",
      Flags = "ok",
      `Dominant in stage` = "no",
      `Likely slowdown drivers` = "N/A"
    )
    complexity_dist_matrix_dt <- data.table::data.table(
      `Complexity class` = "unknown",
      `Function count` = "0",
      `Function share` = "0.0%",
      `Runtime share` = "0.0%",
      Distribution = strrep("-", 24L)
    )
    runtime_dist_matrix_dt <- data.table::data.table(
      Function = "N/A",
      `Relative impact` = "0.0%",
      `Composite score` = "N/A",
      Distribution = strrep("-", 24L)
    )
    bottleneck_lines <- "- None identified."
    top_bottleneck_matrix_dt <- data.table::data.table(
      Rank = "N/A",
      Function = "N/A",
      Complexity = "unknown",
      `Composite score` = "N/A",
      `Stage impact` = "0.0%",
      `adj.R2` = "N/A",
      Confidence = "unknown",
      `Volatility cv` = "N/A",
      `Growth max` = "N/A",
      Flags = "ok",
      `Likely slowdown drivers` = "N/A"
    )
    priority_matrix_dt <- data.table::data.table(
      `Priority tier` = "P2",
      Function = "N/A",
      Complexity = "unknown",
      `Composite score` = "N/A",
      `Stage impact` = "0.0%",
      Reason = "monitor; no acute risk signal",
      `Expected impact` = "N/A",
      Flags = "ok"
    )
    confidence_summary_matrix_dt <- data.table::data.table(
      Metric = c("Mean adjusted R2", "Median adjusted R2"),
      Value = c("N/A", "N/A")
    )
    stage_kpi_matrix_dt <- data.table::data.table(
      KPI = c("Total functions benchmarked", "Dominant complexity class", "Stage risk score"),
      Value = c("0", "unknown", "0")
    )
    stage_narrative_lines <- c(
      "- No benchmarked functions were available for this stage.",
      "- Asymptotic and runtime bottlenecks are not estimable from current data.",
      "- Run benchmarks for this stage before prioritizing optimization work."
    )

    highest_complexity_row <- data.table::data.table(
      fn_name = "N/A",
      best_class = "unknown"
    )
    primary_bottleneck_row <- data.table::data.table(
      fn_name = "N/A",
      best_class = "unknown",
      relative_impact = 0,
      bottleneck_score = NA_real_
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
    sprintf("- Preset: %s", preset_label),
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
      "- Stage risk score: %s",
      .format_metric_value(stage_summary$stage_risk_score[[1L]], digits = 4L)
    ),
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
    "## Stage KPI Dashboard",
    "",
    .build_plaintext_matrix(stage_kpi_matrix_dt),
    "",
    "## Function-Level Performance Matrix",
    "",
    .build_plaintext_matrix(function_matrix_dt),
    "",
    "## Bottleneck Candidates",
    "",
    bottleneck_lines,
    "",
    "## Top Bottlenecks by Composite Score",
    "",
    .build_plaintext_matrix(top_bottleneck_matrix_dt),
    "",
    "## Confidence and Uncertainty Summary",
    "",
    .build_plaintext_matrix(confidence_summary_matrix_dt),
    "",
    "## Optimization Priority Queue",
    "",
    .build_plaintext_matrix(priority_matrix_dt),
    "",
    "## Stage Narrative",
    "",
    stage_narrative_lines,
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
#' @param preset_name A character scalar preset label used for report metadata.
#' @return Character vector of markdown lines.
#' @keywords internal
#' @noRd
.build_general_project_performance_markdown_from_metrics <- function(
  metrics,
  stage_ids,
  preset_name = "custom"
) {
  preset_label <- .sanitize_perf_preset_name(preset_name, default = "custom")

  summary_dt <- data.table::data.table(stage = stage_ids)
  summary_dt <- metrics$stage_summary[summary_dt, on = "stage"]

  defaults <- list(
    function_count = 0L,
    max_complexity = "unknown",
    max_complexity_rank = .complexity_order[["unknown"]],
    stage_runtime_proportion = 0,
    stage_runtime_total_s = 0,
    stage_risk_score = 0,
    low_confidence_function_count = 0L,
    high_volatility_function_count = 0L,
    critical_bottleneck_count = 0L,
    confidence_low_share = 0,
    volatility_share = 0
  )
  for (nm in names(defaults)) {
    if (!nm %in% names(summary_dt)) {
      summary_dt[, (nm) := defaults[[nm]]]
    }
  }

  summary_dt[is.na(function_count), function_count := 0L]
  summary_dt[is.na(max_complexity), max_complexity := "unknown"]
  summary_dt[is.na(max_complexity_rank), max_complexity_rank := .complexity_order[["unknown"]]]
  summary_dt[is.na(stage_runtime_proportion), stage_runtime_proportion := 0]
  summary_dt[is.na(stage_runtime_total_s), stage_runtime_total_s := 0]
  summary_dt[is.na(stage_risk_score), stage_risk_score := 0]
  summary_dt[is.na(low_confidence_function_count), low_confidence_function_count := 0L]
  summary_dt[is.na(high_volatility_function_count), high_volatility_function_count := 0L]
  summary_dt[is.na(critical_bottleneck_count), critical_bottleneck_count := 0L]
  summary_dt[is.na(confidence_low_share), confidence_low_share := 0]
  summary_dt[is.na(volatility_share), volatility_share := 0]

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

  top_runtime_dt <- metrics$function_metrics[
    order(stage, -relative_impact, -bottleneck_score, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "relative_impact")
  ]
  data.table::setnames(
    top_runtime_dt,
    old = c("fn_name", "relative_impact"),
    new = c("top_runtime_fn", "top_runtime_share")
  )

  top_score_dt <- metrics$function_metrics[
    order(stage, -bottleneck_score, -relative_impact, -complexity_rank, fn_name),
    .SD[1L],
    by = stage,
    .SDcols = c("fn_name", "best_class", "bottleneck_score", "priority_tier")
  ]
  data.table::setnames(
    top_score_dt,
    old = c("fn_name", "best_class", "bottleneck_score", "priority_tier"),
    new = c("top_score_fn", "top_score_class", "top_score", "top_priority")
  )

  summary_dt <- top_complexity_dt[summary_dt, on = "stage"]
  summary_dt <- top_runtime_dt[summary_dt, on = "stage"]
  summary_dt <- top_score_dt[summary_dt, on = "stage"]

  summary_dt[is.na(highest_complexity_fn), highest_complexity_fn := "N/A"]
  summary_dt[is.na(highest_complexity_class), highest_complexity_class := "unknown"]
  summary_dt[is.na(top_runtime_fn), top_runtime_fn := "N/A"]
  summary_dt[is.na(top_runtime_share), top_runtime_share := 0]
  summary_dt[is.na(top_score_fn), top_score_fn := "N/A"]
  summary_dt[is.na(top_score_class), top_score_class := "unknown"]
  summary_dt[is.na(top_score), top_score := NA_real_]
  summary_dt[is.na(top_priority), top_priority := "P2"]

  summary_dt[, pipeline := .resolve_stage_report_label(stage)]
  summary_dt[, runtime_share_label := sprintf("%.1f%%", 100 * stage_runtime_proportion)]
  summary_dt[, stage_risk_label := .format_metric_value(stage_risk_score, digits = 4L)]
  summary_dt[, top_runtime_share_label := sprintf("%.1f%%", 100 * top_runtime_share)]
  summary_dt[, top_score_label := .format_metric_value(top_score, digits = 4L)]
  summary_dt[, low_conf_share_label := sprintf("%.1f%%", 100 * confidence_low_share)]
  summary_dt[, volatility_share_label := sprintf("%.1f%%", 100 * volatility_share)]

  summary_matrix_dt <- summary_dt[, .(
    Pipeline = pipeline,
    `Total functions` = as.character(function_count),
    `Highest-complexity function` = sprintf(
      "%s (%s)",
      highest_complexity_fn,
      highest_complexity_class
    ),
    `Dominant complexity` = max_complexity,
    `Identified bottlenecks` = sprintf(
      "%s (%s, score %s)",
      top_score_fn,
      top_score_class,
      top_score_label
    ),
    `Runtime share` = runtime_share_label
  )]

  cross_stage_rank_dt <- summary_dt[order(
    -stage_runtime_proportion,
    -stage_risk_score,
    -max_complexity_rank,
    pipeline
  ), .(
    Pipeline = pipeline,
    `Runtime share` = runtime_share_label,
    `Stage risk score` = stage_risk_label,
    `Dominant complexity` = max_complexity,
    `Top runtime driver` = sprintf("%s (%s)", top_runtime_fn, top_runtime_share_label),
    `Top composite bottleneck` = sprintf("%s (%s)", top_score_fn, top_score_label),
    `Low-confidence share` = low_conf_share_label,
    `High-volatility share` = volatility_share_label,
    `Critical bottlenecks` = as.character(critical_bottleneck_count)
  )]

  stage_bottleneck_matrix_dt <- if ("stage_bottleneck_matrix" %in% names(metrics)) {
    data.table::copy(metrics$stage_bottleneck_matrix)
  } else {
    data.table::data.table()
  }

  if (nrow(stage_bottleneck_matrix_dt) > 0L) {
    stage_bottleneck_matrix_dt <- stage_bottleneck_matrix_dt[
      summary_dt[, .(stage, pipeline)],
      on = "stage"
    ]
    stage_bottleneck_matrix_dt[,
      top_score_label := .format_metric_value(top_score, digits = 4L)
    ]
    stage_bottleneck_matrix_render_dt <- stage_bottleneck_matrix_dt[, .(
      Pipeline = pipeline,
      `Runtime driver` = sprintf("%s (%s)", top_runtime_fn, top_runtime_share_label),
      `Asymptotic driver` = sprintf("%s (%s)", top_complexity_fn, top_complexity_class),
      `Composite-score driver` = sprintf("%s (%s)", top_score_fn, top_score_label),
      Priority = top_priority_tier,
      `Stage risk score` = .format_metric_value(stage_risk_score, digits = 4L),
      `Low-confidence fns` = as.character(low_confidence_function_count),
      `High-volatility fns` = as.character(high_volatility_function_count)
    )]
  } else {
    stage_bottleneck_matrix_render_dt <- data.table::data.table(
      Pipeline = "N/A",
      `Runtime driver` = "N/A",
      `Asymptotic driver` = "N/A",
      `Composite-score driver` = "N/A",
      Priority = "N/A",
      `Stage risk score` = "N/A",
      `Low-confidence fns` = "0",
      `High-volatility fns` = "0"
    )
  }

  global_top_dt <- if ("global_bottlenecks" %in% names(metrics)) {
    data.table::copy(metrics$global_bottlenecks)
  } else {
    data.table::data.table()
  }

  if (nrow(global_top_dt) > 0L) {
    global_top_dt[, pipeline := .resolve_stage_report_label(stage)]
    global_top_dt[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]
    global_top_dt[, score_label := .format_metric_value(bottleneck_score, digits = 4L)]
    global_top_matrix_dt <- global_top_dt[, .(
      Rank = as.character(global_rank),
      Function = fn_name,
      Pipeline = pipeline,
      Complexity = best_class,
      `Composite score` = score_label,
      `Stage impact` = impact_label,
      Confidence = gsub("_", " ", confidence_label, fixed = TRUE),
      `Volatility cv` = .format_metric_value(volatility_cv, digits = 5L),
      Priority = priority_tier,
      Flags = diagnostic_flags,
      `Likely slowdown drivers` = slowdown_drivers
    )]
  } else {
    global_top_matrix_dt <- data.table::data.table(
      Rank = "N/A",
      Function = "N/A",
      Pipeline = "N/A",
      Complexity = "unknown",
      `Composite score` = "N/A",
      `Stage impact` = "0.0%",
      Confidence = "unknown",
      `Volatility cv` = "N/A",
      Priority = "N/A",
      Flags = "ok",
      `Likely slowdown drivers` = "N/A"
    )
  }

  all_fn_dt <- data.table::copy(metrics$function_metrics)
  if (nrow(all_fn_dt) == 0L) {
    all_fn_dt <- data.table::data.table(
      fn_name = character(0),
      stage = character(0),
      best_class = character(0),
      bottleneck_score = numeric(0),
      relative_impact = numeric(0),
      high_complexity_flag = logical(0),
      high_impact_flag = logical(0),
      high_volatility_flag = logical(0),
      low_confidence_flag = logical(0),
      critical_bottleneck_flag = logical(0),
      priority_tier = character(0),
      optimization_reason = character(0),
      expected_impact_label = character(0)
    )
  }

  quick_wins_dt <- all_fn_dt[
    (high_impact_flag | relative_impact >= 0.20) & !high_complexity_flag,
    .(
      fn_name,
      stage,
      best_class,
      bottleneck_score,
      relative_impact,
      optimization_reason,
      expected_impact_label,
      priority_tier
    )
  ][order(-relative_impact, -bottleneck_score, fn_name)]
  quick_wins_dt <- quick_wins_dt[seq_len(min(5L, .N))]

  medium_effort_dt <- all_fn_dt[
    (priority_tier == "P1") |
      ((high_complexity_flag | high_volatility_flag | low_confidence_flag) &
        !critical_bottleneck_flag),
    .(
      fn_name,
      stage,
      best_class,
      bottleneck_score,
      relative_impact,
      optimization_reason,
      expected_impact_label,
      priority_tier
    )
  ][order(-bottleneck_score, -relative_impact, fn_name)]
  medium_effort_dt <- medium_effort_dt[seq_len(min(5L, .N))]

  high_effort_dt <- all_fn_dt[
    critical_bottleneck_flag | (priority_tier == "P0" & high_complexity_flag),
    .(
      fn_name,
      stage,
      best_class,
      bottleneck_score,
      relative_impact,
      optimization_reason,
      expected_impact_label,
      priority_tier
    )
  ][order(-bottleneck_score, -relative_impact, fn_name)]
  high_effort_dt <- high_effort_dt[seq_len(min(5L, .N))]

  to_roadmap_matrix <- function(dt) {
    if (nrow(dt) == 0L) {
      return(data.table::data.table(
        Function = "N/A",
        Pipeline = "N/A",
        Complexity = "unknown",
        `Composite score` = "N/A",
        `Stage impact` = "0.0%",
        Reason = "No candidates identified",
        `Expected impact` = "N/A"
      ))
    }

    dt[, pipeline := .resolve_stage_report_label(stage)]
    dt[, impact_label := sprintf("%.1f%%", 100 * relative_impact)]
    dt[, score_label := .format_metric_value(bottleneck_score, digits = 4L)]

    dt[, .(
      Function = fn_name,
      Pipeline = pipeline,
      Complexity = best_class,
      `Composite score` = score_label,
      `Stage impact` = impact_label,
      Reason = optimization_reason,
      `Expected impact` = expected_impact_label
    )]
  }

  quick_wins_matrix_dt <- to_roadmap_matrix(quick_wins_dt)
  medium_effort_matrix_dt <- to_roadmap_matrix(medium_effort_dt)
  high_effort_matrix_dt <- to_roadmap_matrix(high_effort_dt)

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

  runtime_stage_row <- summary_dt[order(-stage_runtime_proportion, -stage_risk_score)][1L]
  asym_stage_row <- summary_dt[order(-max_complexity_rank, -stage_risk_score)][1L]
  top_global_row <- if (nrow(global_top_dt) > 0L) global_top_dt[1L] else data.table::data.table(
    fn_name = "N/A",
    stage = "N/A",
    score_label = "N/A",
    impact_label = "0.0%"
  )

  project_narrative_lines <- c(
    sprintf(
      "- Runtime is most concentrated in %s (%s of observed cross-stage runtime).",
      runtime_stage_row$pipeline[[1L]],
      runtime_stage_row$runtime_share_label[[1L]]
    ),
    sprintf(
      "- Asymptotic risk is highest in %s (dominant class %s).",
      asym_stage_row$pipeline[[1L]],
      asym_stage_row$max_complexity[[1L]]
    ),
    sprintf(
      "- Optimize first: %s in %s (score %s, impact %s).",
      top_global_row$fn_name[[1L]],
      if (identical(top_global_row$stage[[1L]], "N/A")) {
        "N/A"
      } else {
        .resolve_stage_report_label(top_global_row$stage[[1L]])
      },
      top_global_row$score_label[[1L]],
      top_global_row$impact_label[[1L]]
    )
  )

  c(
    "# General Project Performance",
    "",
    sprintf(
      "- Analysis timestamp (UTC): %s",
      format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    ),
    sprintf("- Preset: %s", preset_label),
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
    .build_plaintext_matrix(summary_matrix_dt),
    "",
    "## Cross-Stage Runtime and Risk Ranking",
    "",
    .build_plaintext_matrix(cross_stage_rank_dt),
    "",
    "## Stage Bottleneck Matrix",
    "",
    .build_plaintext_matrix(stage_bottleneck_matrix_render_dt),
    "",
    "## Global Top Bottleneck Functions",
    "",
    .build_plaintext_matrix(global_top_matrix_dt),
    "",
    "## Recommended Optimization Roadmap",
    "",
    "### Quick Wins",
    "",
    .build_plaintext_matrix(quick_wins_matrix_dt),
    "",
    "### Medium Effort",
    "",
    .build_plaintext_matrix(medium_effort_matrix_dt),
    "",
    "### High Effort / High Impact",
    "",
    .build_plaintext_matrix(high_effort_matrix_dt),
    "",
    "## Project Narrative",
    "",
    project_narrative_lines
  )
}

#' @title Build pipeline markdown bundle
#' @description Build markdown content for all per-pipeline files plus the
#'   project-level summary file.
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @param preset_name A character scalar preset label used for report metadata.
#' @return A named list of character vectors where each name is a markdown
#'   filename and each value is the file content line vector.
build_pipeline_markdown_bundle <- function(results, preset_name = "custom") {
  preset_label <- .sanitize_perf_preset_name(preset_name, default = "custom")
  metrics <- prepare_reporting_metrics(results)

  stage_ids <- .order_stage_ids_for_reports(
    c(
      names(get_perf_pipeline_report_file_map()),
      metrics$stage_summary$stage,
      metrics$function_metrics$stage
    )
  )

  pipeline_reports <- lapply(stage_ids, function(stage_id) {
    .build_pipeline_markdown_from_metrics(
      metrics = metrics,
      stage_id = stage_id,
      preset_name = preset_label
    )
  })
  names(pipeline_reports) <- vapply(
    stage_ids,
    .resolve_stage_report_filename,
    character(1),
    preset_name = preset_label
  )

  summary_filename <- get_perf_general_summary_report_filename(
    preset_name = preset_label
  )

  pipeline_reports[[summary_filename]] <-
    .build_general_project_performance_markdown_from_metrics(
      metrics = metrics,
      stage_ids = stage_ids,
      preset_name = preset_label
    )

  pipeline_reports
}

#' @title Export analysis Markdown
#' @description Write one markdown report per pipeline stage plus a project-level
#'   summary markdown report.
#' @param results A list from run_all_stages() or run_all_benchmarks().
#' @param output_path A character scalar output directory. If a file path ending
#'   in .md is supplied, its parent directory is used.
#' @param preset_name A character scalar preset label used for report metadata.
#' @return An invisible named character vector of written markdown paths.
export_analysis_markdown <- function(
  results,
  output_path,
  preset_name = "custom"
) {
  output_dir <- if (grepl("\\.md$", output_path, ignore.case = TRUE)) {
    dirname(output_path)
  } else {
    output_path
  }

  if (!nzchar(output_dir)) {
    output_dir <- "."
  }

  markdown_bundle <- build_pipeline_markdown_bundle(
    results = results,
    preset_name = preset_name
  )

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
#' @param preset_name A character scalar preset label used for report metadata.
#' @return An invisible character scalar output path.
export_results_markdown <- function(
  results,
  output_path,
  preset_name = "custom"
) {
  export_analysis_markdown(results, output_path, preset_name = preset_name)
}

# -----------------------------------------------------------------------------

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
