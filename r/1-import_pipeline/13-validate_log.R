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
#' table with normalize mandatory columns.
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
  dt_work <- copy_as_data_table(dt)
  mandatory_cols <- config$column_required
  constants <- get_pipeline_constants()

  missing_mandatory_cols <- setdiff(mandatory_cols, colnames(dt_work))

  if (length(missing_mandatory_cols) > 0) {
    dt_work[, (missing_mandatory_cols) := NA_character_]
  }

  if (!"document" %in% colnames(dt_work)) {
    dt_work[, document := constants$defaults$unknown_document]
  }

  document_values <- dt_work[["document"]]
  row_ids <- seq_len(nrow(dt_work))

  error_parts <- vector("list", length(mandatory_cols))
  for (col_idx in seq_along(mandatory_cols)) {
    col <- mandatory_cols[[col_idx]]
    col_values <- dt_work[[col]]
    missing_mask <- is.na(col_values) | col_values == ""
    if (any(missing_mask)) {
      error_parts[[col_idx]] <- paste0(
        "missing mandatory value in document '",
        document_values[missing_mask],
        "', row_id '",
        row_ids[missing_mask],
        "', column '",
        col,
        "'"
      )
    }
  }

  errors <- unique(unlist(error_parts, use.names = FALSE))
  if (is.null(errors)) {
    errors <- character(0)
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
  dt_work <- ensure_data_table(dt)

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

#' @title validate year values
#' @description validate that year values in long-format data fall within a
#' plausible range (1900 to current year + 1) and that year ranges have a
#' non-decreasing start-to-end order.
#' @param dt data table or data frame containing long-format observations with
#' a `year` column.
#' @return named list with `errors` as a character vector of validation messages
#' and `data` as the unchanged data table.
#' @importFrom checkmate assert_data_frame assert_names
#' @importFrom data.table as.data.table
#' @examples
#' dt_example <- data.frame(
#'   product = "wheat", variable = "production",
#'   year = c("2020", "1800", "2025-2020"),
#'   value = c("100", "200", "300"), document = "doc.xlsx"
#' )
#' validate_year_values(dt_example)
validate_year_values <- function(dt) {
  dt_work <- ensure_data_table(dt)
  checkmate::assert_names(colnames(dt_work), must.include = "year")

  current_year <- as.integer(format(Sys.Date(), "%Y"))
  min_year <- 1900L
  max_year <- current_year + 1L

  year_values <- unique(dt_work[["year"]])
  year_values <- year_values[!is.na(year_values) & year_values != ""]

  errors <- character(0)

  for (yr in year_values) {
    if (grepl("^\\d{4}-\\d{4}$", yr)) {
      parts <- strsplit(yr, "-", fixed = TRUE)[[1]]
      start_yr <- as.integer(parts[1])
      end_yr <- as.integer(parts[2])

      if (start_yr > end_yr) {
        errors <- c(
          errors,
          paste0(
            "year range '",
            yr,
            "' has start year greater than end year"
          )
        )
      }

      if (start_yr < min_year || end_yr > max_year) {
        errors <- c(
          errors,
          paste0(
            "year range '",
            yr,
            "' contains year outside plausible range [",
            min_year,
            ", ",
            max_year,
            "]"
          )
        )
      }
    } else if (grepl("^\\d{4}$", yr)) {
      yr_int <- as.integer(yr)
      if (yr_int < min_year || yr_int > max_year) {
        errors <- c(
          errors,
          paste0(
            "year value '",
            yr,
            "' is outside plausible range [",
            min_year,
            ", ",
            max_year,
            "]"
          )
        )
      }
    }
  }

  return(list(errors = errors, data = dt_work))
}


#' @title validate long data table
#' @description run the complete long-table validation pipeline by applying
#' mandatory field checks, year value validation, and duplicate detection, and
#' return a validated table.
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
  year_result <- validate_year_values(mandatory_result$data)
  duplicate_result <- detect_duplicates_dt(year_result$data)

  return(list(
    data = mandatory_result$data,
    errors = c(
      mandatory_result$errors,
      year_result$errors,
      duplicate_result$errors
    )
  ))
}
