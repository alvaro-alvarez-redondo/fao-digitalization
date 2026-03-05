# script: validate log scripts
# description: validate mandatory fields, detect duplicates and collect errors/warnings

#' @title validate mandatory fields data table
#' @description validate mandatory columns in a long-format table, create missing
#' mandatory columns as `na_character_`, ensure a `document` column exists, and
#' generate unique row-level error messages for missing mandatory values.
#' @param dt data table or data frame in long format.
#' @param config named list containing `column_required` as a non-empty character
#' vector.
#' @return named list with `errors` as a character vector and `data` as a data
#' table with normalized mandatory columns.
#' @importFrom checkmate assert_data_frame assert_list assert_character
#' @importFrom data.table as.data.table copy
#' @examples
#' dt_example <- data.frame(
#'   product = "a",
#'   variable = "b",
#'   year = "2020",
#'   value = "",
#'   document = "doc.xlsx"
#' )
#' config_example <- list(column_required = c("product", "variable", "year", "value"))
#' validate_mandatory_fields_dt(dt_example, config_example)
validate_mandatory_fields_dt <- function(dt, config) {
  checkmate::assert_data_frame(dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  )

  # performance: perform a single coercion/copy and avoid `melt()` expansion
  # for mandatory checks, which can inflate memory for large tables.
  dt_work <- data.table::copy(data.table::as.data.table(dt))
  mandatory_cols <- config$column_required

  missing_mandatory_cols <- setdiff(mandatory_cols, colnames(dt_work))

  if (length(missing_mandatory_cols) > 0) {
    dt_work[, (missing_mandatory_cols) := NA_character_]
  }

  if (!"document" %in% colnames(dt_work)) {
    dt_work[, document := "unknown_document"]
  }

  row_count <- nrow(dt_work)
  errors <- character(0)

  if (row_count > 0L) {
    row_ids <- seq_len(row_count)

    missing_error_chunks <- lapply(mandatory_cols, function(column_name) {
      column_values <- dt_work[[column_name]]
      missing_mask <- is.na(column_values) | column_values == ""

      if (!any(missing_mask)) {
        return(character(0))
      }

      missing_rows <- row_ids[missing_mask]
      missing_documents <- dt_work$document[missing_mask]

      # performance: build messages per-column without constructing a melted
      # long table, preserving deterministic row order.
      return(paste0(
        "missing mandatory value in document '",
        missing_documents,
        "', row_id '",
        missing_rows,
        "', column '",
        column_name,
        "'"
      ))
    })

    errors <- unlist(missing_error_chunks, use.names = FALSE)

    if (length(errors) > 0L) {
      errors <- unique(errors)
    }
  }

  return(list(errors = errors, data = dt_work))
}

#' @title detect duplicates data table
#' @description detect duplicate rows using the long-grain key
#' (`product`, `variable`, `year`, `value`, `document`) and return duplicate
#' diagnostics as error messages.
#' @param dt data table or data frame containing long-format observations.
#' @return named list with `errors` as a character vector and `data` as the
#' unchanged data table.
#' @importFrom checkmate assert_data_frame assert_names
#' @importFrom data.table as.data.table
#' @examples
#' dt_example <- data.frame(
#'   product = c("wheat", "wheat"),
#'   variable = c("production", "production"),
#'   year = c("2020", "2020"),
#'   value = c("100", "100"),
#'   document = c("doc.xlsx", "doc.xlsx")
#' )
#' detect_duplicates_dt(dt_example)
detect_duplicates_dt <- function(dt) {
  checkmate::assert_data_frame(dt)
  checkmate::assert_names(
    names(dt),
    must.include = c("product", "variable", "year", "value", "document"),
    what = "names(dt)"
  )

  dt_work <- data.table::as.data.table(dt)

  dup_counts <- dt_work[,
    .(duplicate_count = .N),
    by = .(product, variable, year, value, document)
  ]

  dup_rows <- dup_counts[duplicate_count > 1]

  errors <- if (nrow(dup_rows) > 0) {
    paste0(
      "duplicate entries detected for product '",
      dup_rows$product,
      "', variable '",
      dup_rows$variable,
      "', year '",
      dup_rows$year,
      "', value '",
      dup_rows$value,
      "', duplicate_count '",
      dup_rows$duplicate_count,
      "' in document '",
      dup_rows$document,
      "'"
    )
  } else {
    character(0)
  }

  return(list(errors = errors, data = dt_work))
}

#' @title validate long data table
#' @description run the complete long-table validation pipeline by applying
#' mandatory field checks and duplicate detection, and return a validated table
#' @param long_dt data table or data frame containing long-format records.
#' @param config named list containing `column_required` as a non-empty character
#' vector.
#' @return named list with `data` as a data table and `errors` as a character
#' vector of validation issues.
#' @importFrom checkmate assert_data_frame assert_list assert_character
#' @examples
#' long_dt_example <- data.frame(
#'   product = "a",
#'   variable = "b",
#'   year = "2020",
#'   value = "1",
#'   document = "doc.xlsx"
#' )
#' config_example <- list(column_required = c("product", "variable", "year", "value"))
#' validate_long_dt(long_dt_example, config_example)
validate_long_dt <- function(long_dt, config) {
  checkmate::assert_data_frame(long_dt)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  )

  mandatory_result <- validate_mandatory_fields_dt(long_dt, config)
  duplicate_result <- detect_duplicates_dt(mandatory_result$data)

  return(list(
    data = mandatory_result$data,
    errors = c(mandatory_result$errors, duplicate_result$errors)
  ))
}
