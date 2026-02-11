# ============================================================
# Script:  10-file_io.R
# Purpose: Discover all .xlsx files, validate file names,
#          and extract product/yearbook metadata in a tidy, vectorized style
# ============================================================

# ------------------------------
# Function. Extract metadata from file paths
# ------------------------------
extract_file_metadata <- function(file_paths) {
  tibble::tibble(file_path = file_paths) |>
    dplyr::mutate(
      # Extract file name
      file_name = fs::path_file(file_path),

      # ASCII check
      is_ascii = stringi::stri_enc_isascii(file_name),
      error_message = dplyr::if_else(
        !is_ascii,
        paste0("Non-ASCII file name detected: ", file_name),
        NA_character_
      ),

      # Split name into parts
      name_parts = stringr::str_split(file_name, "_"),

      # Extract metadata using helpers
      yearbook = purrr::map_chr(name_parts, extract_yearbook),
      product = purrr::map_chr(name_parts, extract_product)
    ) |>
    dplyr::select(
      file_path,
      file_name,
      product,
      yearbook,
      is_ascii,
      error_message
    )
}

# ------------------------------
# Function. Discover all .xlsx files given a folder
# ------------------------------
# ============================================================
# Function: discover_files
# Purpose: Search for all .xlsx files in a folder (including subfolders)
#          and return a tidy tibble with file metadata.
# Arguments:
#   - import_folder: character string. Path to the folder to search. Must exist.
# Returns:
#   - A tibble with at least one column 'file_path' and additional metadata
#     extracted by `extract_file_metadata`. If no files found, returns
#     an empty tibble with the same structure.
# ============================================================

discover_files <- function(import_folder) {
  # Validate folder exists
  checkmate::assert_directory_exists(import_folder)

  # Find all .xlsx files recursively
  fs::dir_ls(
    path = import_folder,
    recurse = TRUE,
    type = "file",
    glob = "*.xlsx"
  ) |>
    (\(files_found) {
      if (length(files_found) == 0) {
        warning("No .xlsx files found in import folder: ", import_folder)
        # Return empty tibble with standard column
        return(tibble::tibble(file_path = character()))
      }
      # Extract metadata using dedicated function
      extract_file_metadata(files_found)
    })()
}

# ------------------------------
# Function. Wrapper to integrate with pipeline config
# ------------------------------
discover_pipeline_files <- function(config) {
  config$paths$data$imports$raw |>
    discover_files()
}

# ------------------------------
# End of script
# ------------------------------
