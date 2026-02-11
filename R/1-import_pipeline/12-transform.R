# ============================================================
# Script:  12-transform.R
# Purpose: Normalize metadata, reshape data, and add required metadata columns.
# ============================================================

# ------------------------------
# Function. Identify year columns
# ------------------------------
identify_year_columns <- function(df, config) {
  # Candidate columns exclude known metadata
  candidate_cols <- setdiff(
    colnames(df),
    setdiff(config$column_order, c("year", "value"))
  )
  candidate_cols[grepl("^\\d{4}(-\\d{4})?$", candidate_cols)]
}

# ------------------------------
# Function. Normalize key metadata fields
# ------------------------------
normalize_key_fields <- function(df, product_name, config) {
  base_cols <- config$column_required

  # Add missing columns as NA
  df[, setdiff(base_cols, colnames(df))] <- NA_character_

  df |>
    dplyr::mutate(
      product = normalize_string(product_name),
      variable = normalize_string(variable),
      continent = normalize_string(continent),
      country = normalize_string(country)
    )
}

# ------------------------------
# Function. Convert year columns to character
# ------------------------------
convert_year_columns <- function(df, config) {
  # 1. Clean column names (remove trailing ".0")
  clean_names <- gsub("\\.0$", "", colnames(df))
  df <- data.table::setnames(copy(df), old = colnames(df), new = clean_names)

  # 2. Identify year columns
  year_cols <- identify_year_columns(df, config)

  # 3. Convert year columns to character (vectorized)
  if (length(year_cols) > 0) {
    df[, (year_cols) := lapply(.SD, as.character), .SDcols = year_cols]
  }

  df
}

# ------------------------------
# Function. Reshape data to long format
# ------------------------------
reshape_to_long <- function(df, config) {
  column_id <- config$column_id
  year_cols <- identify_year_columns(df, config)

  data.table::melt(
    data = df,
    id.vars = column_id,
    measure.vars = year_cols,
    variable.name = "year",
    value.name = "value",
    variable.factor = FALSE
  )
}

# ------------------------------
# Function. Add metadata columns
# ------------------------------
add_metadata <- function(dt_long, file_name, yearbook, config) {
  notes_value <- config$defaults$notes_value

  dt_long |>
    dplyr::mutate(
      document = file_name,
      notes = notes_value,
      yearbook = yearbook
    ) |>
    data.table::as.data.table()
}

# ------------------------------
# Function. Transform a single file into wide + long
# ------------------------------
transform_file_dt <- function(df, file_name, yearbook, product_name, config) {
  df_norm <- df |>
    normalize_key_fields(product_name, config) |>
    convert_year_columns(config)

  dt_long <- df_norm |>
    reshape_to_long(config) |>
    add_metadata(file_name, yearbook, config)

  list(wide = df_norm, long = dt_long)
}

# ------------------------------
# Pipeline. Transform single file
# ------------------------------
transform_single_file <- function(file_row, df_wide, config) {
  if (nrow(df_wide) == 0) {
    return(NULL)
  }

  transform_file_dt(
    df = df_wide,
    file_name = file_row$file_name,
    yearbook = file_row$yearbook,
    product_name = file_row$product,
    config = config
  )
}

process_files_with_progress <- function(file_list_dt, read_data_list, config) {
  progressr::handlers(progressr::handler_txtprogressbar(clear = FALSE))

  progressr::with_progress({
    p <- progressr::progressor(along = seq_len(nrow(file_list_dt)))

    purrr::map2(
      seq_len(nrow(file_list_dt)),
      read_data_list,
      function(i, df_wide) {
        file_row <- file_list_dt[i, ]
        p(sprintf("Processing file %d/%d", i, nrow(file_list_dt)))
        transform_single_file(file_row, df_wide, config)
      }
    ) |>
      purrr::compact()
  })
}

process_files_no_progress <- function(file_list_dt, read_data_list, config) {
  purrr::map2(
    seq_len(nrow(file_list_dt)),
    read_data_list,
    function(i, df_wide) {
      transform_single_file(file_list_dt[i, ], df_wide, config)
    }
  ) |>
    purrr::compact()
}

transform_files_list <- function(
  file_list_dt,
  read_data_list,
  config,
  enable_progress = TRUE
) {
  checkmate::assert_data_frame(file_list_dt)
  checkmate::assert_list(read_data_list)
  stopifnot(nrow(file_list_dt) == length(read_data_list))

  results <- if (enable_progress && nrow(file_list_dt) > 0) {
    process_files_with_progress(file_list_dt, read_data_list, config)
  } else {
    process_files_no_progress(file_list_dt, read_data_list, config)
  }

  list(
    wide = purrr::map(results, "wide") |>
      data.table::rbindlist(fill = TRUE),
    long = purrr::map(results, "long") |>
      data.table::rbindlist(fill = TRUE)
  )
}

# ------------------------------
# End of script
# ------------------------------
