#' @title file input-output script
#' @description discover all .xlsx files, validate file names,
#' and extract product/yearbook metadata in a tidy, vectorized style

#' @title extract file metadata
#' @description build a standardized metadata table from a character vector of
#' file paths. the function validates inputs, extracts file names, flags non-ascii
#' names, and derives product and yearbook fields using helper parsers.
#' @param file_paths character vector of file paths. must be non-empty and contain
#' no missing values.
#' @return a tibble with columns `file_path`, `file_name`, `product`, `yearbook`,
#' `is_ascii`, and `error_message`.
#' @importFrom checkmate assert_character
#' @importFrom tibble tibble
#' @importFrom dplyr mutate if_else select
#' @importFrom fs path_file
#' @importFrom stringi stri_enc_isascii
#' @importFrom stringr str_split
#' @importFrom purrr map_chr
#' @examples
#' file_paths_example <- c(
#'   "imports/raw/crops_2020_sample.xlsx",
#'   "imports/raw/livestock_2021_sample.xlsx"
#' )
#' extract_file_metadata(file_paths_example)
extract_file_metadata <- function(file_paths) {
  checkmate::assert_character(file_paths, any.missing = FALSE, min.len = 1)

  tibble::tibble(file_path = file_paths) |>
    dplyr::mutate(
      file_name = fs::path_file(file_path),
      is_ascii = stringi::stri_enc_isascii(file_name),
      error_message = dplyr::if_else(
        !is_ascii,
        paste0("non-ascii file name detected: ", file_name),
        NA_character_
      ),
      name_parts = stringr::str_split(file_name, "_"),
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

#' @title discover files
#' @description recursively discover all xlsx files inside an import directory,
#' validate the directory input, and return standardized metadata for each file.
#' when no files are found, the function emits a cli warning and returns an empty
#' metadata tibble with stable column names.
#' @param import_folder character scalar path to an existing directory.
#' @return a tibble with columns `file_path`, `file_name`, `product`, `yearbook`,
#' `is_ascii`, and `error_message`. returns zero rows when no xlsx files are found.
#' @importFrom checkmate assert_string assert_directory_exists
#' @importFrom fs dir_ls
#' @importFrom cli cli_warn
#' @importFrom tibble tibble
#' @examples
#' temp_import_folder <- tempfile("imports_")
#' fs::dir_create(temp_import_folder)
#' fs::file_create(file.path(temp_import_folder, "crops_2020_sample.xlsx"))
#' discover_files(temp_import_folder)
discover_files <- function(import_folder) {
  checkmate::assert_string(import_folder, min.chars = 1)
  checkmate::assert_directory_exists(import_folder)

  files_found <- fs::dir_ls(
    path = import_folder,
    recurse = TRUE,
    type = "file",
    glob = "*.xlsx"
  )

  if (length(files_found) == 0) {
    cli::cli_warn(c(
      "no xlsx files were found in the import folder",
      "i" = "folder: {import_folder}"
    ))

    return(tibble::tibble(
      file_path = character(),
      file_name = character(),
      product = character(),
      yearbook = character(),
      is_ascii = logical(),
      error_message = character()
    ))
  }

  extract_file_metadata(files_found)
}

#' @title discover pipeline files
#' @description retrieve the raw import folder from a pipeline configuration list,
#' validate configuration structure and target path, and delegate file discovery to
#' `discover_files`.
#' @param config named list containing `paths$data$imports$raw` as a character
#' scalar path to an existing directory.
#' @return a tibble with discovered file metadata from `discover_files`.
#' @importFrom checkmate assert_list assert_string assert_directory_exists
#' @importFrom purrr pluck
#' @examples
#' temp_import_folder <- tempfile("imports_")
#' fs::dir_create(temp_import_folder)
#' config_example <- list(paths = list(data = list(imports = list(raw = temp_import_folder))))
#' discover_pipeline_files(config_example)
discover_pipeline_files <- function(config) {
  checkmate::assert_list(config, any.missing = FALSE)

  import_folder <- purrr::pluck(
    config,
    "paths",
    "data",
    "imports",
    "raw",
    .default = NULL
  )

  checkmate::assert_string(import_folder, min.chars = 1)
  checkmate::assert_directory_exists(import_folder)

  discover_files(import_folder)
}
