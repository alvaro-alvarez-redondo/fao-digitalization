# script: transform script
# description: normalize metadata, reshape data, and add required metadata columns

#' @title identify year columns
#' @description identify columns that represent year values in four-digit format
#' or year ranges in `yyyy-yyyy` format, excluding known metadata columns from
#' the configured output schema.
#' @param df data frame or data table with source columns.
#' @param config named list containing `column_order` as a character vector.
#' @return character vector of detected year column names.
#' @importFrom checkmate assert_data_frame assert_list assert_character
#' @examples
#' df_example <- data.frame(country = "x", `2020` = "1", `2020-2021` = "2")
#' config_example <- list(column_order = c("country", "year", "value"))
#' identify_year_columns(df_example, config_example)
identify_year_columns <- function(df, config) {
  checkmate::assert_data_frame(df)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1
  )

  candidate_cols <- setdiff(
    colnames(df),
    setdiff(config$column_order, c("year", "value"))
  )

  candidate_cols[grepl("^\\d{4}(-\\d{4})?$", candidate_cols)]
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
#' @importFrom checkmate assert_data_frame assert_string assert_list assert_character
#' @importFrom dplyr mutate
#' @examples
#' df_example <- data.frame(variable = "yield", continent = "asia", country = "india")
#' config_example <- list(column_required = c("product", "variable", "continent", "country"))
#' normalize_key_fields(df_example, "crops", config_example)
normalize_key_fields <- function(df, product_name, config) {
  checkmate::assert_data_frame(df)
  checkmate::assert_string(product_name, min.chars = 1)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_required,
    any.missing = FALSE,
    min.len = 1
  )

  base_cols <- config$column_required
  df[, setdiff(base_cols, colnames(df))] <- NA_character_

  df |>
    dplyr::mutate(
      product = normalize_string(product_name),
      variable = normalize_string(variable),
      continent = normalize_string(continent),
      country = normalize_string(country)
    )
}

#' @title convert year columns
#' @description sanitize column names by removing trailing `.0`, identify year
#' columns, and coerce year columns to character for consistent downstream
#' reshaping.
#' @param df data table or data frame with potential year columns.
#' @param config named list containing `column_order` as a character vector.
#' @return input table with cleaned names and year columns converted to character.
#' @importFrom checkmate assert_data_frame assert_list assert_character
#' @importFrom data.table setnames
#' @examples
#' df_example <- data.frame(country = "x", `2020.0` = 1)
#' config_example <- list(column_order = c("country", "year", "value"))
#' convert_year_columns(df_example, config_example)
convert_year_columns <- function(df, config) {
  checkmate::assert_data_frame(df)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1
  )

  clean_names <- gsub("\\.0$", "", colnames(df))

  if (!identical(clean_names, colnames(df))) {
    data.table::setnames(df, old = colnames(df), new = clean_names)
  }

  year_cols <- identify_year_columns(df, config)

  if (length(year_cols) > 0) {
    df[, (year_cols) := lapply(.SD, as.character), .SDcols = year_cols]
  }

  df
}

#' @title reshape to long
#' @description reshape wide source data into long format using configured id
#' columns and detected year columns.
#' @param df data table or data frame in wide format.
#' @param config named list containing `column_id` as a character vector and
#' `column_order` for year detection.
#' @return data table with `year` and `value` columns created by melting.
#' @importFrom checkmate assert_data_frame assert_list assert_character
#' @importFrom data.table melt
#' @importFrom cli cli_abort
#' @examples
#' df_example <- data.frame(country = "x", `2020` = "1")
#' config_example <- list(column_id = "country", column_order = c("country", "year", "value"))
#' reshape_to_long(df_example, config_example)
reshape_to_long <- function(df, config) {
  checkmate::assert_data_frame(df)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_character(
    config$column_id,
    any.missing = FALSE,
    min.len = 1
  )
  checkmate::assert_character(
    config$column_order,
    any.missing = FALSE,
    min.len = 1
  )

  column_id <- config$column_id
  year_cols <- identify_year_columns(df, config)

  if (length(year_cols) == 0) {
    cli::cli_abort("no year columns were identified for reshaping")
  }

  data.table::melt(
    data = df,
    id.vars = column_id,
    measure.vars = year_cols,
    variable.name = "year",
    value.name = "value",
    variable.factor = FALSE
  )
}

#' @title add metadata
#' @description append file-level metadata fields (`document`, `notes`,
#' `yearbook`) to long-format data and return a data table.
#' @param fao_data_long_raw data frame or data table in long format.
#' @param file_name character scalar source file name.
#' @param yearbook character scalar yearbook label.
#' @param config named list containing `defaults$notes_value` as a character
#' scalar and may be `na_character_` when notes are intentionally missing.
#' @return data table with metadata columns added.
#' @importFrom checkmate assert_data_frame assert_string assert_list assert_character
#' @importFrom dplyr mutate
#' @importFrom data.table as.data.table
#' @examples
#' df_example <- data.frame(country = "x", year = "2020", value = "1")
#' config_example <- list(defaults = list(notes_value = NA_character_))
#' add_metadata(df_example, "file.xlsx", "yearbook_a", config_example)
add_metadata <- function(fao_data_long_raw, file_name, yearbook, config) {
  checkmate::assert_data_frame(fao_data_long_raw)
  checkmate::assert_string(file_name, min.chars = 1)
  checkmate::assert_string(yearbook, min.chars = 1)
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_list(config$defaults, any.missing = FALSE)
  checkmate::assert_character(config$defaults$notes_value, len = 1)

  notes_value <- config$defaults$notes_value

  fao_data_long_raw |>
    dplyr::mutate(
      document = file_name,
      notes = notes_value,
      yearbook = yearbook
    ) |>
    data.table::as.data.table()
}

#' @title transform file data table
#' @description run normalization, year coercion, long reshaping, and metadata
#' enrichment for one file dataset.
#' @param df data frame or data table with source observations.
#' @param file_name character scalar source file name.
#' @param yearbook character scalar yearbook label.
#' @param product_name character scalar product label.
#' @param config named list containing required transform configuration.
#' @return named list with `wide_raw` and `long_raw` data tables.
#' @importFrom checkmate assert_data_frame assert_string assert_list
#' @examples
#' # transform_file_dt(df_example, "file.xlsx", "yearbook_a", "crops", config_example)
transform_file_dt <- function(df, file_name, yearbook, product_name, config) {
  checkmate::assert_data_frame(df)
  checkmate::assert_string(file_name, min.chars = 1)
  checkmate::assert_string(yearbook, min.chars = 1)
  checkmate::assert_string(product_name, min.chars = 1)
  checkmate::assert_list(config, any.missing = FALSE)

  df_norm <- df |>
    normalize_key_fields(product_name, config) |>
    convert_year_columns(config)

  fao_data_long_raw <- df_norm |>
    reshape_to_long(config) |>
    add_metadata(file_name, yearbook, config)

  list(wide_raw = df_norm, long_raw = fao_data_long_raw)
}

#' @title transform single file
#' @description transform one discovered file row and its wide data table. returns
#' `null` when the input table has zero rows.
#' @param file_row single-row data frame or data table with `file_name`,
#' `yearbook`, and `product` columns.
#' @param df_wide data frame or data table for one file.
#' @param config named list with transform configuration.
#' @return named list from `transform_file_dt` or `null` when `df_wide` is empty.
#' @importFrom checkmate assert_data_frame assert_list assert_names assert_flag
#' @importFrom purrr pluck
#' @importFrom cli cli_warn
#' @examples
#' # transform_single_file(file_row_example, df_wide_example, config_example)
transform_single_file <- function(file_row, df_wide, config) {
  checkmate::assert_data_frame(file_row, min.rows = 1)
  checkmate::assert_names(
    names(file_row),
    must.include = c("file_name", "yearbook", "product"),
    what = "names(file_row)"
  )
  checkmate::assert_data_frame(df_wide)
  checkmate::assert_list(config, any.missing = FALSE)

  warn_missing_product <- purrr::pluck(
    config,
    "defaults",
    "warn_missing_product",
    .default = FALSE
  )
  checkmate::assert_flag(warn_missing_product)

  if (nrow(df_wide) == 0) {
    return(NULL)
  }

  product_name <- file_row$product[[1]]

  if (is.na(product_name) || product_name == "") {
    if (warn_missing_product) {
      cli::cli_warn(c(
        "missing product metadata detected; using fallback value 'unknown'",
        "i" = "file: {file_row$file_name[[1]]}"
      ))
    }
    product_name <- "unknown"
  }

  transform_file_dt(
    df = df_wide,
    file_name = file_row$file_name,
    yearbook = file_row$yearbook,
    product_name = product_name,
    config = config
  )
}

#' @title process files
#' @description iterate over discovered files and corresponding read datasets,
#' apply `transform_single_file`, and collect non-null results with a progress
#' indicator.
#' @param file_list_dt data frame or data table describing files.
#' @param read_data_list list of data frames or data tables aligned to file rows.
#' @param config named list with transform configuration.
#' @return list of transformed per-file results.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom progressr handlers handler_txtprogressbar with_progress progressor
#' @importFrom purrr map2 compact
#' @examples
#' # process_files(file_list_dt_example, read_data_list_example, config_example)
process_files <- function(file_list_dt, read_data_list, config) {
  checkmate::assert_data_frame(file_list_dt)
  checkmate::assert_list(read_data_list)
  checkmate::assert_list(config, any.missing = FALSE)

  progressr::handlers(progressr::handler_txtprogressbar(clear = FALSE))

  progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(nrow(file_list_dt)))

    purrr::map2(
      seq_len(nrow(file_list_dt)),
      read_data_list,
      \(i, df_wide) {
        file_row <- file_list_dt[i, ]
        progress(sprintf("processing file %d/%d", i, nrow(file_list_dt)))
        transform_single_file(file_row, df_wide, config)
      }
    ) |>
      purrr::compact()
  })
}

#' @title transform files list
#' @description transform all discovered files by combining metadata rows with
#' read datasets, then bind transformed wide and long outputs into unified data
#' tables.
#' @param file_list_dt data frame or data table with file metadata rows.
#' @param read_data_list list of per-file data frames or data tables.
#' @param config named list with transform configuration.
#' @return named list with `wide_raw` and `long_raw` as data tables.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom cli cli_abort
#' @importFrom data.table data.table rbindlist
#' @importFrom purrr map
#' @examples
#' file_list_example <- data.frame(file_name = character(), yearbook = character(), product = character())
#' read_data_list_example <- list()
#' config_example <- list()
#' transform_files_list(file_list_example, read_data_list_example, config_example)
transform_files_list <- function(file_list_dt, read_data_list, config) {
  checkmate::assert_data_frame(file_list_dt)
  checkmate::assert_list(read_data_list)
  checkmate::assert_list(config, any.missing = FALSE)

  if (nrow(file_list_dt) != length(read_data_list)) {
    cli::cli_abort("file list row count must match read data list length")
  }

  if (nrow(file_list_dt) == 0) {
    return(list(
      wide_raw = data.table::data.table(),
      long_raw = data.table::data.table()
    ))
  }

  results <- process_files(file_list_dt, read_data_list, config)

  list(
    wide_raw = results |>
      purrr::map("wide_raw") |>
      data.table::rbindlist(fill = TRUE),
    long_raw = results |>
      purrr::map("long_raw") |>
      data.table::rbindlist(fill = TRUE)
  )
}
