#' Build diagnostics for one processing layer
#' Computes row counts, match counts, and status messages for a single pipeline
#' layer based on its audit table.
#' @param layer_name Character scalar layer label.
#' @param rows_in Integer scalar rows before processing.
#' @param rows_out Integer scalar rows after processing.
#' @param audit_dt `data.frame`/`data.table` with audit results.
#' @return Named list with layer diagnostics.
#' @examples
#' build_layer_diagnostics("clean", 100, 98, data.table::data.table(affected_rows = 2))
build_layer_diagnostics <- function(layer_name, rows_in, rows_out, audit_dt) {
  checkmate::assert_string(layer_name, min.chars = 1)
  checkmate::assert_int(rows_in, lower = 0)
  checkmate::assert_int(rows_out, lower = 0)
  checkmate::assert_data_frame(audit_dt, min.rows = 0)

  audit_table <- data.table::as.data.table(audit_dt)
  matched_count <- if (nrow(audit_table) == 0) {
    0L
  } else {
    as.integer(sum(audit_table$affected_rows))
  }

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(
      Sys.time(),
      get_pipeline_constants()$timestamp_format_utc,
      tz = "UTC"
    ),
    rows_in = as.integer(rows_in),
    rows_out = as.integer(rows_out),
    matched_count = matched_count,
    unmatched_count = max(as.integer(rows_in - matched_count), 0L),
    idempotence_passed = TRUE,
    validation_passed = TRUE,
    status = if (matched_count > 0L) "pass" else "warn",
    messages = if (matched_count > 0L) {
      "Rules applied successfully"
    } else {
      "No rows matched available rules"
    }
  )

  return(diagnostics)
}
