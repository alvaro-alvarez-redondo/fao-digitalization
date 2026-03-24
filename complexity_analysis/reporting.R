# complexity_analysis/reporting.R
# description: reporting layer for the Big O complexity analysis module.
#   provides console output (print_complexity_report), JSON export
#   (export_results_json), and optional PNG plots (write_complexity_plots).
#   the JSON serialiser is pure R with no external dependencies; ggplot2
#   is used for plots when available, with a base-R fallback.
#
# depends on: aggregation.R (get_complexity_order, get_overall_pipeline_class)
# sourced by:  complexity_analysis/run_analysis.R

# ── reporting layer ──────────────────────────────────────────────────────────

#' @title print complexity report
#' @description prints a formatted console table of complexity results with
#'   a summary of the dominant pipeline stage and overall complexity.
#' @param complexity_dt data.table from `run_all_benchmarks()$complexity`.
#' @return invisible(NULL)
print_complexity_report <- function(complexity_dt) {
  cat("\n")
  cat(strrep("─", 70L), "\n")
  cat("  WHEP PIPELINE — EMPIRICAL BIG O ANALYSIS\n")
  cat(strrep("─", 70L), "\n\n")

  w_fn    <- max(nchar(complexity_dt$fn_name), 12L) + 2L
  w_stage <- max(nchar(complexity_dt$stage),   10L) + 2L
  w_class <- max(nchar(complexity_dt$best_class), 10L) + 2L
  w_r2    <- 10L
  w_flag  <- 8L

  header <- sprintf(
    "%-*s %-*s %-*s %*s %*s",
    w_fn, "FUNCTION", w_stage, "STAGE", w_class, "COMPLEXITY",
    w_r2, "adj.R²", w_flag, "DOMINANT"
  )
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")

  for (i in seq_len(nrow(complexity_dt))) {
    row <- complexity_dt[i]
    r2  <- if (is.na(row$r_squared)) "   N/A" else sprintf("%6.4f", row$r_squared)
    flag <- if (isTRUE(row$dominant_in_stage)) "  ★" else ""
    cat(sprintf(
      "%-*s %-*s %-*s %*s %*s\n",
      w_fn, row$fn_name, w_stage, row$stage, w_class, row$best_class,
      w_r2, r2, w_flag, flag
    ))
  }

  cat("\n")
  cat(strrep("─", 70L), "\n")
  cat("  PER-STAGE DOMINANT COMPLEXITY\n")
  cat(strrep("─", 70L), "\n")

  complexity_order <- get_complexity_order()
  stage_summary <- complexity_dt[
    dominant_in_stage == TRUE,
    .(dominant_class = {
      classes <- sort(unique(best_class))
      ranked  <- match(classes, complexity_order, nomatch = 7L)
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

  overall <- get_overall_pipeline_class(complexity_dt)
  cat("\n")
  cat(sprintf("  OVERALL PIPELINE ESTIMATE: %s\n", overall))
  cat(strrep("─", 70L), "\n\n")

  return(invisible(NULL))
}

#' @title export results as JSON
#' @description writes a structured JSON file summarising the complexity
#'   analysis results. uses a minimal pure-R JSON serialiser to avoid
#'   depending on `jsonlite`.
#' @param results list returned by `run_all_benchmarks()`.
#' @param output_path character scalar — path for the JSON file.
#' @return invisible character scalar — path where JSON was written.
export_results_json <- function(results, output_path) {
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
    dom    <- s_rows[dominant_in_stage == TRUE]
    list(
      functions      = s_rows$fn_name,
      dominant_class = if (nrow(dom) > 0L) dom$best_class[[1L]] else "unknown"
    )
  })
  names(per_stage) <- stages

  overall_class <- get_overall_pipeline_class(complexity_dt)

  report <- list(
    analysis_timestamp     = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    overall_pipeline_class = overall_class,
    per_stage              = per_stage,
    per_function           = per_fn
  )

  # ── minimal pure-R JSON serialiser ─────────────────────────────────────────
  to_json <- function(x, indent = 0L) {
    pad  <- strrep("  ", indent)
    pad2 <- strrep("  ", indent + 1L)

    if (is.null(x)) return("null")

    if (is.logical(x) && length(x) == 1L) {
      return(if (x) "true" else "false")
    }
    if (is.numeric(x) && length(x) == 1L) {
      if (is.nan(x) || is.infinite(x)) return("null")
      return(sprintf("%.15g", x))
    }
    if (is.character(x) && length(x) == 1L) {
      escaped <- gsub("\\", "\\\\", x, fixed = TRUE)
      escaped <- gsub('"',  '\\"',  escaped, fixed = TRUE)
      escaped <- gsub("\n", "\\n",  escaped, fixed = TRUE)
      escaped <- gsub("\r", "\\r",  escaped, fixed = TRUE)
      escaped <- gsub("\t", "\\t",  escaped, fixed = TRUE)
      escaped <- gsub("\f", "\\f",  escaped, fixed = TRUE)
      escaped <- gsub("\b", "\\b",  escaped, fixed = TRUE)
      # escape remaining C0 control characters (0x01–0x1f, excluding the five
      # handled above: \b=8, \t=9, \n=10, \f=12, \r=13). cp=0 (NUL) is
      # excluded because rawToChar(as.raw(0)) returns "" and gsub("", ...) is
      # a zero-length pattern error; NUL bytes cannot appear in R strings.
      for (cp in setdiff(1L:31L, c(8L, 9L, 10L, 12L, 13L))) {
        chr <- rawToChar(as.raw(cp))
        if (!nzchar(chr)) next
        escaped <- gsub(chr, sprintf("\\u%04x", cp), escaped, fixed = TRUE)
      }
      return(paste0('"', escaped, '"'))
    }
    if (is.character(x) || is.numeric(x) || is.logical(x)) {
      elems <- vapply(x, function(v) to_json(v, indent + 1L), character(1))
      return(paste0(
        "[\n", paste0(pad2, elems, collapse = ",\n"), "\n", pad, "]"
      ))
    }
    if (is.list(x)) {
      nms <- names(x)
      if (!is.null(nms) && length(nms) == length(x) && all(nzchar(nms))) {
        pairs <- mapply(
          function(k, v) paste0(pad2, '"', k, '": ', to_json(v, indent + 1L)),
          nms, x,
          SIMPLIFY = TRUE, USE.NAMES = FALSE
        )
        return(paste0("{\n", paste(pairs, collapse = ",\n"), "\n", pad, "}"))
      } else {
        elems <- lapply(x, function(v) paste0(pad2, to_json(v, indent + 1L)))
        return(paste0("[\n", paste(elems, collapse = ",\n"), "\n", pad, "]"))
      }
    }
    return(paste0('"', as.character(x), '"'))
  }

  json_str <- to_json(report)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(json_str, con = output_path)
  message(sprintf("  JSON summary written to: %s", output_path))
  return(invisible(output_path))
}

#' @title produce runtime plots
#' @description saves one plot per benchmark function showing median elapsed
#'   time vs. input size, with the fitted complexity class in the title.
#'   uses ggplot2 when available, otherwise falls back to base R graphics.
#' @param results list returned by `run_all_benchmarks()`.
#' @param plots_dir character scalar — directory for PNG output files.
#' @return invisible character vector — paths of produced plot files.
write_complexity_plots <- function(results, plots_dir) {
  dir.create(plots_dir, recursive = TRUE, showWarnings = FALSE)
  summary_dt    <- results$summary
  complexity_dt <- results$complexity

  fn_names    <- unique(summary_dt$fn_name)
  use_ggplot  <- requireNamespace("ggplot2", quietly = TRUE)
  produced_files <- character(0L)

  for (fn in fn_names) {
    s_dt   <- summary_dt[fn_name == fn][order(n)]
    c_row  <- complexity_dt[fn_name == fn]
    r2_val  <- if (nrow(c_row) > 0L) c_row$r_squared[[1L]]  else NA_real_
    cls_val <- if (nrow(c_row) > 0L) c_row$best_class[[1L]] else "?"
    title   <- sprintf(
      "%s\n%s  (adj.R²=%s)", fn, cls_val,
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
            x = s_dt$n, y = s_dt$median_s * 1000,
            type = "n", main = title,
            xlab = "Input size  n", ylab = "Median elapsed time  (ms)", las = 1L
          )
          graphics::polygon(
            x = c(s_dt$n, rev(s_dt$n)),
            y = c(s_dt$min_s, rev(s_dt$max_s)) * 1000,
            col = grDevices::adjustcolor("#2166AC", alpha.f = 0.15), border = NA
          )
          graphics::lines(s_dt$n, s_dt$median_s * 1000, col = "#2166AC", lwd = 1.5)
          graphics::points(s_dt$n, s_dt$median_s * 1000,
                           col = "#D73027", pch = 19L, cex = 1.2)
        }

        grDevices::dev.off()
        produced_files <- c(produced_files, out_path)
      },
      error = function(e) {
        tryCatch(grDevices::dev.off(), error = function(e2) NULL)
        warning(sprintf("plot failed for '%s': %s", fn, conditionMessage(e)))
      }
    )
  }

  message(sprintf("  %d plot(s) written to: %s", length(produced_files), plots_dir))
  return(invisible(produced_files))
}
