# ============================================================
# Script:  11-reading.R
# Purpose: Read all sheets from .xlsx files, enforce base columns,
#          and return tidy data.tables with error logging
# ============================================================

# ------------------------------
# Function. Read a single sheet
# ------------------------------
read_excel_sheet <- function(file_path, sheet_name) {
  df <- suppressMessages(
    readxl::read_excel(
      path = file_path,
      sheet = sheet_name,
      col_names = TRUE,
      col_types = "text" # force all columns to character
    )
  )

  base_cols <- c("continent", "country", "unit")
  missing_base <- setdiff(base_cols, colnames(df))
  errors <- character(0)

  if (length(missing_base) > 0) {
    errors <- c(
      errors,
      paste0(
        "Sheet '",
        sheet_name,
        "' missing base columns: ",
        paste(missing_base, collapse = ", "),
        " in file '",
        fs::path_file(file_path),
        "'"
      )
    )
    df[missing_base] <- NA_character_
  }

  df_filtered <- df |>
    dplyr::filter(dplyr::if_any(all_of(base_cols), ~ !is.na(.x) & .x != "")) |>
    dplyr::mutate(variable = sheet_name)

  list(data = data.table::as.data.table(df_filtered), errors = errors)
}

# ------------------------------
# Function. Read all sheets from a single file
# ------------------------------
read_file_sheets <- function(file_path) {
  sheets <- readxl::excel_sheets(file_path)
  if (length(sheets) == 0) {
    return(list(data = data.table::data.table(), errors = character(0)))
  }

  non_ascii <- sheets[!stringi::stri_enc_isascii(sheets)]
  errors <- if (length(non_ascii) > 0) {
    paste0(
      "Non-ASCII sheet names in file '",
      fs::path_file(file_path),
      "': ",
      paste(non_ascii, collapse = ", ")
    )
  } else {
    character(0)
  }

  sheets_list <- purrr::map(sheets, ~ read_excel_sheet(file_path, .x))

  combined_data <- sheets_list |>
    purrr::map("data") |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)

  combined_errors <- c(errors, sheets_list |> purrr::map("errors") |> unlist())

  list(data = combined_data, errors = combined_errors)
}

# ------------------------------
# Function. Read multiple files
# ------------------------------
read_pipeline_files <- function(file_list_dt) {
  if (nrow(file_list_dt) == 0) {
    return(list(data = data.table::data.table(), errors = character(0)))
  }

  results <- purrr::map(seq_len(nrow(file_list_dt)), function(i) {
    read_file_sheets(file_list_dt$file_path[i])
  })

  combined_data <- results |>
    purrr::map("data") |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)

  combined_errors <- results |>
    purrr::map("errors") |>
    unlist()

  list(data = combined_data, errors = combined_errors)
}

# ------------------------------
# End of script
# ------------------------------
