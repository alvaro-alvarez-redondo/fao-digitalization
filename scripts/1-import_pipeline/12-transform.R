# script: transform script
# description: normalize metadata, reshape data, and add required metadata columns

#' @title identify year columns
#' @description identify columns that represent year values in four-digit format
#' or year ranges in `yyyy-yyyy` format, excluding known metadata columns from
#' the configured output schema.
#' @param df data frame or data table with source columns.
#' @param config named list containing `column_order` as a character vector.
#' @return character vector of detected year column names.
#' @importFrom checkmate check_character check_data_frame check_list
#' @examples
#' df_example <- data.frame(country = "x", `2020` = "1", `2020-2021` = "2")
#' config_example <- list(column_order = c("country", "year", "value"))
#' identify_year_columns(df_example, config_example)
identify_year_columns <- function(df, config) {
  candidate_cols <- setdiff(
    colnames(df),
    setdiff(config$column_order, c("year", "value"))
  )

  year_columns <- candidate_cols[grepl("^\\d{4}(-\\d{4})?$", candidate_cols)]

  return(year_columns)
}

#' @title normalize key fields
#' @description ensure required base columns exist and normalize key text fields
#' (`product`, `variable`, `continent`, `country`) using `normalize_string`.
#' missing required columns are created with `na_character_`.
#' @param df data frame or data table to normalize.
#' @param product_name character scalar product label from file metadata.
#' @param config named list containing `column_required` as a non-empty character
#' vector.
#' @return data frame with required columns present and normalized key text fields.
#' @importFrom checkmate check_character check_data_frame check_list check_string
#' @examples
#' df_example <- data.frame(variable = "yield", continent = "asia", country = "india")
#' config_example <- list(column_required = c("product", "variable", "continent", "country"))
#' normalize_key_fields(df_example, "crops", config_example)
normalize_key_fields <- function(df, product_name, config) {
  data_dt <- ensure_data_table(df)
  base_cols <- config$column_required
  missing_cols <- setdiff(base_cols, colnames(data_dt))

  if (length(missing_cols) > 0) {
    data_dt[, (missing_cols) := NA_character_]
  }

  data_dt[, product := normalize_string_impl(product_name)]

  # normalize data-sourced text columns (product is set from file metadata above)
  norm_cols <- c("variable", "hemisphere", "continent", "country")
  for (col in norm_cols) {
    if (col %in% colnames(data_dt)) {
      data.table::set(
        data_dt,
        j = col,
        value = normalize_string_impl(data_dt[[col]])
      )
    }
  }

  return(data_dt)
}

#' @title convert year columns
#' @description sanitize column names by removing trailing `.0`, identify year
#' columns, and coerce year columns to character for consistent downstream
#' reshaping.
#' @param df data table or data frame with potential year columns.
#' @param config named list containing `column_order` as a character vector.
#' @return input table with cleaned names and year columns converted to character.
#' @importFrom checkmate check_character check_data_frame check_list
#' @importFrom data.table setnames
#' @examples
#' df_example <- data.frame(country = "x", `2020.0` = 1)
#' config_example <- list(column_order = c("country", "year", "value"))
#' convert_year_columns(df_example, config_example)
convert_year_columns <- function(df, config) {
  clean_names <- gsub("\\.0$", "", colnames(df))
  clean_names <- sub("^(\\d{4})-\\d{2}$", "\\1", clean_names)
  clean_names <- sub(
    "^(\\d{4})-\\d{2}/(\\d{4})-\\d{2}$",
    "\\1-\\2",
    clean_names
  )

  if (!identical(clean_names, colnames(df))) {
    data.table::setnames(df, old = colnames(df), new = clean_names)
  }

  year_cols <- identify_year_columns(df, config)

  if (length(year_cols) > 0) {
    non_char_cols <- year_cols[
      !vapply(year_cols, function(col) is.character(df[[col]]), logical(1))
    ]
    if (length(non_char_cols) > 0) {
      for (col in non_char_cols) {
        data.table::set(df, j = col, value = as.character(df[[col]]))
      }
    }
  }

  return(df)
}

#' @title reshape to long
#' @description reshape wide source data into long format using configured id
#' columns and detected year columns.
#' @param df data table or data frame in wide format.
#' @param config named list containing `column_id` as a character vector and
#' `column_order` for year detection.
#' @return data table with `year` and `value` columns created by melting.
#' @importFrom checkmate check_character check_data_frame check_list
#' @importFrom data.table melt
#' @importFrom cli cli_abort
#' @examples
#' df_example <- data.frame(country = "x", `2020` = "1")
#' config_example <- list(column_id = "country", column_order = c("country", "year", "value"))
#' reshape_to_long(df_example, config_example)
reshape_to_long <- function(df, config) {
  column_id <- config$column_id
  data_dt <- ensure_data_table(df)

  # only keep id columns that are actually present in the data; this allows
  # optional columns like "hemisphere" to be carried through when present
  # without causing errors in files that legitimately omit them
  column_id <- intersect(column_id, colnames(data_dt))

  year_cols <- identify_year_columns(data_dt, config)

  if (length(year_cols) == 0) {
    cli::cli_abort("no year columns were identified for reshaping")
  }

  long_dt <- data.table::melt(
    data = data_dt,
    id.vars = column_id,
    measure.vars = year_cols,
    variable.name = "year",
    value.name = "value",
    variable.factor = FALSE
  )

  return(long_dt)
}

#' @title add metadata
#' @description append file-level metadata fields (`document`, `notes`,
#' `yearbook`) to long-format data using `data.table::set()` for zero-overhead
#' column assignment.
#' @param whep_data_long_raw data table in long format (as returned by
#' `reshape_to_long()`).
#' @param file_name character scalar source file name.
#' @param yearbook character scalar yearbook label.
#' @param config named list containing `defaults$notes_value` as a character
#' scalar and may be `na_character_` when notes are intentionally missing.
#' @return data table with metadata columns added (modified in place).
#' @importFrom checkmate check_character check_data_frame check_list check_string
#' @importFrom data.table set
#' @examples
#' df_example <- data.frame(country = "x", year = "2020", value = "1")
#' config_example <- list(defaults = list(notes_value = NA_character_))
#' add_metadata(df_example, "file.xlsx", "yearbook_a", config_example)
add_metadata <- function(whep_data_long_raw, file_name, yearbook, config) {
  notes_value <- config$defaults$notes_value
  data_dt <- ensure_data_table(whep_data_long_raw)

  data.table::set(data_dt, j = "document", value = file_name)
  data.table::set(data_dt, j = "notes", value = notes_value)
  data.table::set(data_dt, j = "yearbook", value = yearbook)

  return(data_dt)
}

#' @title transform file data table
#' @description run normalization, year coercion, long reshaping, metadata
#' enrichment, and NA-value filtering for one file dataset. filtering NA rows
#' here avoids carrying throwaway rows into downstream binding and validation.
#' @param df data frame or data table with source observations.
#' @param file_name character scalar source file name.
#' @param yearbook character scalar yearbook label.
#' @param product_name character scalar product label.
#' @param config named list containing required transform configuration.
#' @return named list with `wide_raw` and `long_raw` data tables.
#' @importFrom checkmate check_data_frame check_list check_string
#' @examples
#' # transform_file_dt(df_example, "file.xlsx", "yearbook_a", "crops", config_example)
transform_file_dt <- function(df, file_name, yearbook, product_name, config) {
  assert_or_abort(checkmate::check_data_frame(df))
  assert_or_abort(checkmate::check_string(file_name, min.chars = 1))
  assert_or_abort(checkmate::check_string(yearbook, min.chars = 1))
  assert_or_abort(checkmate::check_string(product_name, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  df_norm <- df |>
    normalize_key_fields(product_name, config) |>
    convert_year_columns(config)

  whep_data_long_raw <- df_norm |>
    reshape_to_long(config) |>
    add_metadata(file_name, yearbook, config) |>
    drop_na_value_rows()

  transform_result <- list(wide_raw = df_norm, long_raw = whep_data_long_raw)
  assert_transform_result_contract(transform_result)

  return(transform_result)
}

#' @title resolve product name from file metadata
#' @description resolve the product value from a single metadata row. when the
#' product field is missing or blank, return `"unknown"` and optionally emit a
#' warning based on configuration.
#' @param file_row single-row data frame or data table with `file_name`,
#' `yearbook`, and `product` columns.
#' @param config named list with transform configuration.
#' @return character scalar product name resolved from metadata.
#' @importFrom checkmate check_data_frame check_list
#' @importFrom cli cli_warn
#' @examples
#' # resolve_product_name(file_row_example, config_example)
resolve_product_name <- function(file_row, config) {
  show_missing_product_metadata_warning <-
    !is.null(config$messages$show_missing_product_metadata_warning) &&
    isTRUE(config$messages$show_missing_product_metadata_warning)

  product_name <- as.character(file_row[["product"]][[1]])
  product_name <- trimws(product_name)

  if (is.na(product_name) || product_name == "") {
    if (show_missing_product_metadata_warning) {
      cli::cli_warn(c(
        "missing product metadata detected; using fallback value 'unknown'",
        "i" = "file: {file_row[['file_name']][[1]]}"
      ))
    }

    return("unknown")
  }

  return(product_name)
}

#' @title build empty transform result
#' @description create a stable empty output structure for transform list
#' operations.
#' @return named list with empty `wide_raw` and `long_raw` data tables.
#' @importFrom data.table data.table
build_empty_transform_result <- function() {
  transform_result <- list(
    wide_raw = data.table::data.table(),
    long_raw = data.table::data.table()
  )

  assert_transform_result_contract(transform_result)

  return(transform_result)
}


#' @title assert transform result contract
#' @description validate the stable transform output structure used across import
#' pipeline stages.
#' @param transform_result named list expected to include data.table elements
#' `wide_raw` and `long_raw`.
#' @return invisible `TRUE` when the contract is valid.
#' @importFrom checkmate check_list check_names check_data_table
assert_transform_result_contract <- function(transform_result) {
  assert_or_abort(checkmate::check_list(transform_result, any.missing = FALSE))
  assert_or_abort(checkmate::check_names(
    names(transform_result),
    must.include = c("wide_raw", "long_raw")
  ))
  assert_or_abort(checkmate::check_data_table(transform_result$wide_raw))
  assert_or_abort(checkmate::check_data_table(transform_result$long_raw))

  return(invisible(TRUE))
}

#' @title transform single file
#' @description transform one discovered file row and its wide data table. returns
#' `null` when the input table has zero rows.
#' @param file_row single-row data frame or data table with `file_name`,
#' `yearbook`, and `product` columns.
#' @param df_wide data frame or data table for one file.
#' @param config named list with transform configuration.
#' @return named list from `transform_file_dt` or `null` when `df_wide` is empty.
#' @importFrom checkmate check_data_frame check_list check_names
#' @examples
#' # transform_single_file(file_row_example, df_wide_example, config_example)
transform_single_file <- function(file_row, df_wide, config) {
  assert_or_abort(checkmate::check_data_frame(
    file_row,
    min.rows = 1,
    max.rows = 1
  ))
  assert_or_abort(checkmate::check_names(
    names(file_row),
    must.include = c("file_name", "yearbook", "product"),
    what = "names(file_row)"
  ))
  assert_or_abort(checkmate::check_data_frame(df_wide))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  if (nrow(df_wide) == 0) {
    return(NULL)
  }

  product_name <- resolve_product_name(file_row, config)

  transformed <- transform_file_dt(
    df = df_wide,
    file_name = file_row[["file_name"]],
    yearbook = file_row[["yearbook"]],
    product_name = product_name,
    config = config
  )

  return(transformed)
}

#' @title process files
#' @description iterate over discovered files and corresponding read datasets,
#' apply `transform_single_file`, and collect non-null results. when a `future`
#' parallel backend is configured, transformations run in parallel using
#' `future.apply::future_lapply()`.
#' @param file_list_dt data frame or data table describing files.
#' @param read_data_list list of data frames or data tables aligned to file rows.
#' @param config named list with transform configuration.
#' @param progressor optional `progressr::progressor()` function used to advance a
#' shared import-pipeline progress bar. when `NULL`, no progress update is emitted
#' by this function.
#' @return list of transformed per-file results.
#' @importFrom checkmate check_data_frame check_list check_function
#' @importFrom future.apply future_lapply
#' @examples
#' # process_files(file_list_dt_example, read_data_list_example, config_example)
process_files <- function(
  file_list_dt,
  read_data_list,
  config,
  progressor = NULL
) {
  assert_or_abort(checkmate::check_data_frame(file_list_dt))
  assert_or_abort(checkmate::check_list(read_data_list))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  if (!is.null(progressor)) {
    assert_or_abort(checkmate::check_function(progressor))
  }

  expected_items <- nrow(file_list_dt)
  provided_items <- length(read_data_list)

  if (provided_items != expected_items) {
    cli::cli_abort(c(
      "{.arg read_data_list} length must match {.arg file_list_dt} rows",
      "x" = "rows in file_list_dt: {expected_items}",
      "x" = "elements in read_data_list: {provided_items}"
    ))
  }

  invalid_read_data_index <- 0L
  for (i in seq_along(read_data_list)) {
    if (!is.data.frame(read_data_list[[i]])) {
      invalid_read_data_index <- i
      break
    }
  }

  if (invalid_read_data_index > 0) {
    cli::cli_abort(c(
      "all elements in {.arg read_data_list} must be data.frame-compatible objects",
      "x" = "invalid element index: {invalid_read_data_index}"
    ))
  }

  use_parallel <- !inherits(future::plan(), "sequential") &&
    expected_items > 1L

  indices <- seq_len(expected_items)

  # pre-split file_list_dt into a list of single-row data.tables once
  # to avoid repeated row-slicing (which triggers implicit coercion
  # via as.data.frame.character/numeric/POSIXct in hot loops)
  file_list_dt <- ensure_data_table(file_list_dt)
  file_rows_list <- lapply(indices, function(i) file_list_dt[i])

  if (use_parallel) {
    results <- future.apply::future_lapply(
      indices,
      function(index) {
        file_row <- file_rows_list[[index]]
        df_wide <- read_data_list[[index]]

        transform_single_file(file_row, df_wide, config)
      },
      future.seed = NULL
    )
  } else {
    results <- lapply(
      indices,
      function(index) {
        file_row <- file_rows_list[[index]]
        df_wide <- read_data_list[[index]]

        if (!is.null(progressor)) {
          progressor(sprintf(
            "Import Pipeline Progress: transforming %s",
            file_row[["file_name"]]
          ))
        }

        transform_single_file(file_row, df_wide, config)
      }
    )
  }

  results <- Filter(Negate(is.null), results)

  return(results)
}

#' @title transform files list
#' @description transform all discovered files by combining metadata rows with
#' read datasets, then bind transformed wide and long outputs into unified data
#' tables.
#' @param file_list_dt data frame or data table with file metadata rows.
#' @param read_data_list list of per-file data frames or data tables.
#' @param config named list with transform configuration.
#' @param progressor optional `progressr::progressor()` function used to advance a
#' shared import-pipeline progress bar. when `NULL`, no progress update is emitted
#' by this function.
#' @return named list with `wide_raw` and `long_raw` as data tables.
#' @importFrom checkmate check_data_frame check_list check_function
#' @importFrom cli cli_abort
#' @importFrom data.table data.table rbindlist
#' @examples
#' file_list_example <- data.frame(file_name = character(), yearbook = character(), product =
#' character())
#' read_data_list_example <- list()
#' config_example <- list()
#' transform_files_list(file_list_example, read_data_list_example, config_example)
transform_files_list <- function(
  file_list_dt,
  read_data_list,
  config,
  progressor = NULL
) {
  assert_or_abort(checkmate::check_data_frame(file_list_dt))
  assert_or_abort(checkmate::check_list(read_data_list))
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  if (!is.null(progressor)) {
    assert_or_abort(checkmate::check_function(progressor))
  }

  if (nrow(file_list_dt) != length(read_data_list)) {
    cli::cli_abort("file list row count must match read data list length")
  }

  if (nrow(file_list_dt) == 0) {
    return(build_empty_transform_result())
  }

  results <- process_files(
    file_list_dt,
    read_data_list,
    config,
    progressor = progressor
  )

  if (length(results) == 0) {
    return(build_empty_transform_result())
  }

  n_results <- length(results)
  wide_list <- vector("list", n_results)
  long_list <- vector("list", n_results)
  for (i in seq_len(n_results)) {
    wide_list[[i]] <- results[[i]][["wide_raw"]]
    long_list[[i]] <- results[[i]][["long_raw"]]
  }

  transformed <- list(
    wide_raw = data.table::rbindlist(wide_list, use.names = TRUE, fill = TRUE),
    long_raw = data.table::rbindlist(long_list, use.names = TRUE, fill = TRUE)
  )

  assert_transform_result_contract(transformed)

  return(transformed)
}
