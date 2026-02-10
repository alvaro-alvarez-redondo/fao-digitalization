# ============================================================
# Script:  10-file_io.R
# Purpose: Discover all .xlsx files, validate file names,
#          and extract product/yearbook metadata in a tidy, vectorized style
# ============================================================

# ------------------------------
# Function. Extract metadata from file names
# ------------------------------
extract_file_metadata <- function(file_paths) {
  tibble::tibble(file_path = file_paths) |>
    dplyr::mutate(
      file_name = fs::path_file(file_path),
      ascii_ok = stringi::stri_enc_isascii(file_name),
      error_message = dplyr::if_else(
        !ascii_ok,
        paste0("Non-ASCII file name detected: ", file_name),
        NA_character_
      ),
      name_parts = stringr::str_split(file_name, "_"),
      product = purrr::map_chr(name_parts, ~ fs::path_ext_remove(tail(.x, 1))),
      yearbook = purrr::map_chr(
        name_parts,
        ~ {
          if (length(.x) >= 4) paste(.x[2:4], collapse = "_") else NA_character_
        }
      )
    ) |>
    dplyr::select(file_path, file_name, product, yearbook, error_message)
}

# ------------------------------
# Function. Discover all .xlsx files given a folder
# ------------------------------
discover_files <- function(import_folder) {
  checkmate::assert_directory_exists(import_folder)

  files_found <- fs::dir_ls(
    path = import_folder,
    recurse = TRUE,
    type = "file",
    glob = "*.xlsx"
  )

  if (length(files_found) == 0) {
    warning("No .xlsx files found in import folder: ", import_folder)
    return(tibble::tibble())
  }

  extract_file_metadata(files_found)
}

# ------------------------------
# Function. Wrapper to integrate with pipeline config
# ------------------------------
discover_pipeline_files <- function(config) {
  raw_import_path <- config$paths$data$imports$raw
  discover_files(raw_import_path)
}

# ------------------------------
# End of script
# ------------------------------
