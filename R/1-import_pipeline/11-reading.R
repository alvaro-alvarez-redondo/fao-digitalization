# ============================================================
# Script:  11-reading.R
# Purpose: Read all sheets from .xlsx files, enforce base columns,
#          and return tidy data.tables with error logging
# ============================================================

# ------------------------------
# Function. Read a single sheet
# ------------------------------
read_excel_sheet <- function(file_path, sheet_name, config) {
  base_cols <- config$column_required
  safe_read_result <- tryCatch(
    {
      suppressMessages(
        readxl::read_excel(
          file_path,
          sheet = sheet_name,
          col_names = TRUE,
          col_types = "text"
        )
      )
    },
    error = function(condition) {
      return(structure(list(error_message = condition$message), class = "read_error"))
    }
  )

  if (inherits(safe_read_result, "read_error")) {
    return(list(
      data = data.table::data.table(),
      errors = paste0(
        "failed to read sheet '",
        sheet_name,
        "' in file '",
        fs::path_file(file_path),
        "': ",
        safe_read_result$error_message
      )
    ))
  }

  missing_base <- setdiff(base_cols, colnames(safe_read_result))
  missing_base_errors <- if (length(missing_base) > 0) {
    paste0(
      "sheet '",
      sheet_name,
      "' missing base columns: ",
      paste(missing_base, collapse = ", "),
      " in file '",
      fs::path_file(file_path),
      "'"
    )
  } else {
    character(0)
  }

  safe_read_result[missing_base] <- NA_character_

  list(
    data = safe_read_result |>
      dplyr::filter(dplyr::if_any(
        tidyselect::all_of(base_cols),
        ~ !is.na(.x) & .x != ""
      )) |>
      dplyr::mutate(variable = sheet_name) |>
      data.table::as.data.table(),
    errors = missing_base_errors
  )
}

# ------------------------------
# Function. Read all sheets from a single file
# ------------------------------
read_file_sheets <- function(file_path, config) {
  sheets <- tryCatch(
    readxl::excel_sheets(file_path),
    error = function(condition) {
      return(structure(list(error_message = condition$message), class = "read_error"))
    }
  )

  if (inherits(sheets, "read_error")) {
    return(list(
      data = data.table::data.table(),
      errors = paste0(
        "failed to list sheets in file '",
        fs::path_file(file_path),
        "': ",
        sheets$error_message
      )
    ))
  }

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

  sheets_list <- purrr::map(sheets, ~ read_excel_sheet(file_path, .x, config))

  combined_data <- sheets_list |>
    purrr::map("data") |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)

  combined_errors <- c(errors, sheets_list |> purrr::map("errors") |> unlist())

  list(data = combined_data, errors = combined_errors)
}

# ------------------------------
# Function. Read multiple files
# ------------------------------
read_pipeline_files <- function(file_list_dt, config) {
  if (nrow(file_list_dt) == 0) {
    return(list(read_data_list = list(), errors = character(0)))
  }

  read_results <- purrr::map(file_list_dt$file_path, ~ read_file_sheets(.x, config))
  parsed_results <- purrr::transpose(read_results)

  list(
    read_data_list = parsed_results$data,
    errors = parsed_results$errors |> unlist(use.names = FALSE)
  )
}

# ------------------------------
# End of script
# ------------------------------
