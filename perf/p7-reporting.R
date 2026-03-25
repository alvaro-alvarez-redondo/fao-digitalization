# module:      p7-reporting
# description: reporting, visualisation, and persistence for the perf
#   module. provides console printers for per-stage and global
#   diagnostics, a structured JSON exporter, per-stage PNG plot writer, and
#   a function to persist the full analysis object to a .qs file.
#
# depends on:  (no intra-module deps — operates on result lists and data.tables)
# sourced by:  perf/run_perf.R

# ── 8. reporting and persistence ─────────────────────────────────────────────

# ── 8a. console reporters ─────────────────────────────────────────────────────

#' @title print per-stage complexity report
#' @description prints a formatted console table for one stage diagnostic,
#'   including ranked bottlenecks, dominant complexity, scaling description,
#'   and optimization signals.
#' @param stage_diagnostic named list from `diagnose_stage()`.
#' @return invisible(NULL)
print_stage_report <- function(stage_diagnostic) {
  sid   <- stage_diagnostic$stage_id
  dom   <- stage_diagnostic$dominant_class
  cdt   <- stage_diagnostic$complexity_dt
  bns   <- stage_diagnostic$bottlenecks
  sigs  <- stage_diagnostic$optimization_signals

  cat("\n")
  cat(strrep("\u2500", 70L), "\n")
  cat(sprintf("  STAGE: %s\n", toupper(sid)))
  cat(strrep("\u2500", 70L), "\n")
  cat(sprintf("  Dominant complexity : %s\n", dom))
  cat(sprintf("  Scaling behaviour   : %s\n", stage_diagnostic$scaling_description))
  if (isTRUE(stage_diagnostic$is_concern)) {
    cat("  \u26a0  CONCERN: This stage contains at least one super-linear function.\n")
  }

  # per-function table
  w_fn    <- max(nchar(cdt$fn_name),  12L) + 2L
  w_class <- max(nchar(cdt$best_class), 10L) + 2L
  w_r2    <- 10L
  w_flag  <- 8L
  cat("\n")
  header <- sprintf("  %-*s %-*s %*s %*s",
                    w_fn, "FUNCTION", w_class, "COMPLEXITY",
                    w_r2, "adj.R\u00b2", w_flag, "DOMINANT")
  cat(header, "\n")
  cat(sprintf("  %s\n", strrep("-", nchar(header) - 2L)))

  for (i in seq_len(nrow(cdt))) {
    row  <- cdt[i]
    r2   <- if (is.na(row$r_squared)) "   N/A" else sprintf("%6.4f", row$r_squared)
    flag <- if (isTRUE(row$dominant_in_stage)) "  \u2605" else ""
    cat(sprintf("  %-*s %-*s %*s %*s\n",
                w_fn,    row$fn_name,
                w_class, row$best_class,
                w_r2, r2,
                w_flag, flag))
  }

  # top bottlenecks
  if (nrow(bns) > 0L) {
    cat(sprintf("\n  Top %d bottleneck(s):\n", nrow(bns)))
    for (i in seq_len(nrow(bns))) {
      cat(sprintf("    %d. %s  [%s]\n", i, bns$fn_name[[i]], bns$best_class[[i]]))
    }
  }

  # optimization signals
  cat("\n  Optimization signals:\n")
  for (nm in names(sigs)) {
    cat(sprintf("    \u2192 %s\n", sigs[[nm]]))
  }

  return(invisible(NULL))
}

#' @title print global pipeline complexity report
#' @description prints a formatted console summary of the global complexity
#'   diagnostic, including a cross-stage comparison, overall pipeline class,
#'   and the dominant bottleneck function.
#' @param global_diagnostic named list from `build_global_diagnostic()`.
#' @return invisible(NULL)
print_global_report <- function(global_diagnostic) {
  gd  <- global_diagnostic
  sdt <- gd$stage_summary

  cat("\n")
  cat(strrep("\u2550", 70L), "\n")
  cat("  WHEP PIPELINE \u2014 UNIFIED COMPLEXITY DIAGNOSTIC\n")
  cat(strrep("\u2550", 70L), "\n\n")

  cat("  CROSS-STAGE SUMMARY\n")
  cat(strrep("-", 50L), "\n")
  for (i in seq_len(nrow(sdt))) {
    concern <- if (isTRUE(sdt$is_concern[[i]])) "  \u26a0" else ""
    cat(sprintf("  %-28s  %-15s%s\n",
                sdt$stage[[i]], sdt$dominant_class[[i]], concern))
  }

  cat("\n")
  cat(sprintf("  OVERALL PIPELINE CLASS : %s\n", gd$overall_class))
  cat(sprintf("  Scaling behaviour      : %s\n", gd$overall_scaling_description))
  if (!is.na(gd$pipeline_bottleneck)) {
    cat(sprintf("  Primary bottleneck     : %s\n", gd$pipeline_bottleneck))
  }
  cat(strrep("\u2550", 70L), "\n\n")

  return(invisible(NULL))
}

#' @title print flat complexity report (backward-compatible)
#' @description prints the flat complexity report format from the original
#'   `97-reporting.R` in the legacy scripts. accepts the `complexity_dt`
#'   data.table produced by `run_all_benchmarks()`.
#' @param complexity_dt data.table from `run_all_benchmarks()$complexity`.
#' @return invisible(NULL)
print_complexity_report <- function(complexity_dt) {
  cat("\n")
  cat(strrep("\u2500", 70L), "\n")
  cat("  WHEP PIPELINE \u2014 EMPIRICAL BIG O ANALYSIS\n")
  cat(strrep("\u2500", 70L), "\n\n")

  w_fn    <- max(nchar(complexity_dt$fn_name),  12L) + 2L
  w_stage <- max(nchar(complexity_dt$stage),    10L) + 2L
  w_class <- max(nchar(complexity_dt$best_class), 10L) + 2L
  w_r2    <- 10L
  w_flag  <- 8L

  header <- sprintf("%-*s %-*s %-*s %*s %*s",
                    w_fn, "FUNCTION", w_stage, "STAGE",
                    w_class, "COMPLEXITY", w_r2, "adj.R\u00b2",
                    w_flag, "DOMINANT")
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")

  for (i in seq_len(nrow(complexity_dt))) {
    row  <- complexity_dt[i]
    r2   <- if (is.na(row$r_squared)) "   N/A" else sprintf("%6.4f", row$r_squared)
    flag <- if (isTRUE(row$dominant_in_stage)) "  \u2605" else ""
    cat(sprintf("%-*s %-*s %-*s %*s %*s\n",
                w_fn,    row$fn_name,
                w_stage, row$stage,
                w_class, row$best_class,
                w_r2, r2,
                w_flag, flag))
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
      ranked  <- match(classes, complexity_order_vec, nomatch = 7L)
      classes[[which.max(ranked)]]
    }),
    by = stage
  ][order(stage)]

  for (i in seq_len(nrow(stage_summary))) {
    cat(sprintf("  %-25s  %s\n",
                stage_summary$stage[[i]],
                stage_summary$dominant_class[[i]]))
  }

  all_ranks <- match(stage_summary$dominant_class, complexity_order_vec, nomatch = 7L)
  overall   <- stage_summary$dominant_class[[which.max(all_ranks)]]
  cat("\n")
  cat(sprintf("  OVERALL PIPELINE ESTIMATE: %s\n", overall))
  cat(strrep("\u2500", 70L), "\n\n")

  return(invisible(NULL))
}

# ── 8b. JSON export ───────────────────────────────────────────────────────────

#' @title minimal R-to-JSON serialiser
#' @description pure-R recursive JSON serialiser with no external dependencies.
#'   handles NULL, logical scalars, numeric scalars (with NaN/Inf → null),
#'   character scalars (with full C0 escape), atomic vectors as arrays, and
#'   named/unnamed lists as objects/arrays.
#' @param x R value to serialise.
#' @param indent integer. current indentation depth.
#' @return character scalar — JSON fragment.
.to_json <- function(x, indent = 0L) {
  pad  <- strrep("  ", indent)
  pad2 <- strrep("  ", indent + 1L)

  if (is.null(x)) return("null")

  if (is.logical(x) && length(x) == 1L)
    return(if (x) "true" else "false")

  if (is.numeric(x) && length(x) == 1L) {
    if (is.nan(x) || is.infinite(x)) return("null")
    return(sprintf("%.15g", x))
  }

  if (is.character(x) && length(x) == 1L) {
    esc <- gsub("\\",  "\\\\", x, fixed = TRUE)
    esc <- gsub('"',   '\\"',  esc, fixed = TRUE)
    esc <- gsub("\n",  "\\n",  esc, fixed = TRUE)
    esc <- gsub("\r",  "\\r",  esc, fixed = TRUE)
    esc <- gsub("\t",  "\\t",  esc, fixed = TRUE)
    esc <- gsub("\f",  "\\f",  esc, fixed = TRUE)
    esc <- gsub("\b",  "\\b",  esc, fixed = TRUE)
    for (cp in setdiff(1L:31L, c(8L, 9L, 10L, 12L, 13L))) {
      chr <- rawToChar(as.raw(cp))
      if (!nzchar(chr)) next
      esc <- gsub(chr, sprintf("\\u%04x", cp), esc, fixed = TRUE)
    }
    return(paste0('"', esc, '"'))
  }

  if (is.character(x) || is.numeric(x) || is.logical(x)) {
    elems <- vapply(x, function(v) .to_json(v, indent + 1L), character(1))
    return(paste0("[\n", paste0(pad2, elems, collapse = ",\n"), "\n", pad, "]"))
  }

  if (is.list(x)) {
    nms <- names(x)
    if (!is.null(nms) && length(nms) == length(x) && all(nzchar(nms))) {
      pairs <- mapply(
        function(k, v) paste0(pad2, '"', k, '": ', .to_json(v, indent + 1L)),
        nms, x,
        SIMPLIFY = TRUE, USE.NAMES = FALSE
      )
      return(paste0("{\n", paste(pairs, collapse = ",\n"), "\n", pad, "}"))
    } else {
      elems <- lapply(x, function(v) paste0(pad2, .to_json(v, indent + 1L)))
      return(paste0("[\n", paste(elems, collapse = ",\n"), "\n", pad, "]"))
    }
  }

  return(paste0('"', as.character(x), '"'))
}

#' @title export analysis results as JSON
#' @description writes a structured JSON file summarising the full complexity
#'   analysis results. the JSON captures per-function and per-stage information
#'   together with the overall pipeline class. no external dependencies required.
#' @param results list from `run_all_stages()` or `run_all_benchmarks()`.
#' @param output_path character scalar — destination file path.
#' @return invisible character scalar — path where JSON was written.
export_analysis_json <- function(results, output_path) {
  complexity_dt <- results$complexity

  per_fn <- lapply(seq_len(nrow(complexity_dt)), function(i) {
    row <- complexity_dt[i]
    list(
      fn_name           = row$fn_name,
      stage             = row$stage,
      description       = row$description,
      best_class        = row$best_class,
      adj_r_squared     = if (is.na(row$r_squared)) NULL else round(row$r_squared, 6L),
      slope_per_n       = if (is.na(row$slope_per_n)) NULL else row$slope_per_n,
      dominant_in_stage = isTRUE(row$dominant_in_stage)
    )
  })
  names(per_fn) <- complexity_dt$fn_name

  stages    <- unique(complexity_dt$stage)
  per_stage <- lapply(stages, function(s) {
    s_rows <- complexity_dt[stage == s]
    dom    <- s_rows[isTRUE(dominant_in_stage) | dominant_in_stage == TRUE]
    list(
      functions      = s_rows$fn_name,
      dominant_class = if (nrow(dom) > 0L) dom$best_class[[1L]] else "unknown"
    )
  })
  names(per_stage) <- stages

  dom_classes  <- vapply(per_stage, function(s) s$dominant_class, character(1))
  overall_rank <- match(dom_classes, names(.complexity_order),
                        nomatch = .complexity_order[["unknown"]])
  overall_class <- dom_classes[[which.max(overall_rank)]]

  report <- list(
    analysis_timestamp     = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    overall_pipeline_class = overall_class,
    per_stage              = per_stage,
    per_function           = per_fn
  )

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(.to_json(report), con = output_path)
  message(sprintf("  JSON summary written to: %s", output_path))
  return(invisible(output_path))
}

#' @title backward-compatible alias for export_analysis_json
#' @description wraps `export_analysis_json()` under the original name used by
#'   the legacy `97-reporting.R` script so existing callers and tests
#'   are unaffected.
#' @param results list from `run_all_stages()` or `run_all_benchmarks()`.
#' @param output_path character scalar — destination file path.
#' @return invisible character scalar — path where JSON was written.
export_results_json <- function(results, output_path) {
  export_analysis_json(results, output_path)
}

# ── 8c. plot writer ───────────────────────────────────────────────────────────

#' @title write per-function runtime plots
#' @description saves one PNG plot per benchmark function showing median elapsed
#'   time vs. input size with a min-max shaded band. uses ggplot2 when
#'   available, otherwise falls back to base R graphics.
#' @param results list with `summary` and `complexity` data.tables.
#' @param plots_dir character scalar — directory for PNG output.
#' @return invisible character vector — paths of produced plot files.
write_stage_plots <- function(results, plots_dir) {
  dir.create(plots_dir, recursive = TRUE, showWarnings = FALSE)
  summary_dt    <- results$summary
  complexity_dt <- results$complexity
  fn_names      <- unique(summary_dt$fn_name)
  use_ggplot    <- requireNamespace("ggplot2", quietly = TRUE)
  produced      <- character(0L)

  for (fn in fn_names) {
    s_dt  <- summary_dt[fn_name == fn][order(n)]
    c_row <- complexity_dt[fn_name == fn]
    r2_val  <- if (nrow(c_row) > 0L) c_row$r_squared[[1L]] else NA_real_
    cls_val <- if (nrow(c_row) > 0L) c_row$best_class[[1L]] else "?"
    title   <- sprintf(
      "%s\n%s  (adj.R\u00b2=%s)",
      fn, cls_val,
      if (is.na(r2_val)) "NA" else sprintf("%.3f", r2_val)
    )
    out_path <- file.path(plots_dir, paste0(fn, ".png"))

    tryCatch(
      {
        grDevices::png(out_path, width = 800L, height = 500L, res = 100L)
        if (use_ggplot) {
          p <- ggplot2::ggplot(s_dt, ggplot2::aes(x = n, y = median_s * 1000)) +
            ggplot2::geom_ribbon(
              ggplot2::aes(ymin = min_s * 1000, ymax = max_s * 1000),
              alpha = 0.15, fill = "#2166AC"
            ) +
            ggplot2::geom_line(colour = "#2166AC", linewidth = 0.9) +
            ggplot2::geom_point(colour = "#D73027", size = 2.5) +
            ggplot2::labs(
              title   = title,
              x       = "Input size  n",
              y       = "Median elapsed time  (ms)",
              caption = sprintf(
                "stage: %s  |  reps per n: %d",
                if (nrow(c_row) > 0L) c_row$stage[[1L]] else "?",
                nrow(results$raw[fn_name == fn]) / length(unique(s_dt$n))
              )
            ) +
            ggplot2::theme_bw(base_size = 12L) +
            ggplot2::theme(plot.title = ggplot2::element_text(size = 11L))
          print(p)
        } else {
          graphics::plot(
            x = s_dt$n, y = s_dt$median_s * 1000, type = "n",
            main = title, xlab = "Input size  n",
            ylab = "Median elapsed time  (ms)", las = 1L
          )
          graphics::polygon(
            x = c(s_dt$n, rev(s_dt$n)),
            y = c(s_dt$min_s, rev(s_dt$max_s)) * 1000,
            col = grDevices::adjustcolor("#2166AC", alpha.f = 0.15),
            border = NA
          )
          graphics::lines(s_dt$n, s_dt$median_s * 1000,
                          col = "#2166AC", lwd = 1.5)
          graphics::points(s_dt$n, s_dt$median_s * 1000,
                           col = "#D73027", pch = 19L, cex = 1.2)
        }
        grDevices::dev.off()
        produced <- c(produced, out_path)
      },
      error = function(e) {
        tryCatch(grDevices::dev.off(), error = function(e2) NULL)
        warning(sprintf("plot failed for '%s': %s", fn, conditionMessage(e)))
      }
    )
  }

  message(sprintf("  %d plot(s) written to: %s", length(produced), plots_dir))
  return(invisible(produced))
}

#' @title backward-compatible alias for write_stage_plots
#' @description wraps `write_stage_plots()` under the name used by the original
#'   legacy `97-reporting.R` script.
#' @param results list with `summary` and `complexity` data.tables.
#' @param plots_dir character scalar — directory for PNG output.
#' @return invisible character vector — paths of produced plot files.
write_complexity_plots <- function(results, plots_dir) {
  write_stage_plots(results, plots_dir)
}

# ── 8d. .qs persistence ───────────────────────────────────────────────────────

#' @title persist full analysis object to .qs file
#' @description saves `analysis_object` to `qs_path` using `qs::qsave()` for
#'   fast, compressed serialisation. creates the parent directory when needed.
#'   if the `qs` package is not available, falls back to `saveRDS()` with a
#'   warning so the pipeline does not hard-fail.
#' @param analysis_object any R object — typically the list returned by
#'   `run_big_o_analysis()`.
#' @param qs_path character scalar — destination file path (should end in .qs).
#' @return invisible character scalar — path where the object was written.
persist_analysis <- function(analysis_object, qs_path) {
  dir.create(dirname(qs_path), recursive = TRUE, showWarnings = FALSE)

  if (requireNamespace("qs", quietly = TRUE)) {
    qs::qsave(analysis_object, qs_path)
    message(sprintf("  Analysis object saved to: %s", qs_path))
  } else {
    rds_path <- sub("\\.qs$", ".rds", qs_path)
    warning(
      "package 'qs' not available; falling back to saveRDS(). ",
      "Install qs for faster serialisation.\n",
      "Object saved to: ", rds_path
    )
    saveRDS(analysis_object, rds_path)
    qs_path <- rds_path
    message(sprintf("  Analysis object saved to: %s", qs_path))
  }

  return(invisible(qs_path))
}
