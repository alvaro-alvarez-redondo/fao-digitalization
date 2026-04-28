# script: file input-output script
# description: discover all .xlsx files, validate file names,
# and extract product/yearbook metadata in a vectorized data.table style

#' @title build empty file metadata table
#' @description create a zero-row data.table with the stable schema used by
#' file discovery helpers.
#' @return a zero-row data.table with columns `file_path`, `file_name`,
#' `product`, `yearbook`, `is_ascii`, and `error_message`.
#' @importFrom data.table data.table
build_empty_file_metadata <- function() {
  return(data.table::data.table(
    file_path = character(),
    file_name = character(),
    product = character(),
    yearbook = character(),
    is_ascii = logical(),
    error_message = character()
  ))
}

#' @title extract file metadata
#' @description build a standardized metadata table from a character vector of
#' file paths. the function validates inputs, extracts file names, flags non-ascii
#' names, and derives product and yearbook fields using helper parsers.
#' @param file_paths character vector of file paths. must be non-empty and contain
#' no missing values.
#' @return a data.table with columns `file_path`, `file_name`, `product`,
#' `yearbook`, `is_ascii`, and `error_message`.
#' @importFrom checkmate check_character
#' @importFrom data.table data.table
#' @importFrom fs path_file
#' @importFrom stringi stri_enc_isascii
#' @examples
#' file_paths_example <- c(
#'   "1-import/10-raw_import/crops_2020_sample.xlsx",
#'   "1-import/10-raw_import/livestock_2021_sample.xlsx"
#' )
#' extract_file_metadata(file_paths_example)
extract_file_metadata <- function(file_paths) {
  assert_or_abort(checkmate::check_character(
    file_paths,
    any.missing = FALSE,
    min.len = 1
  ))

  file_name <- fs::path_file(file_paths)
  is_ascii <- stringi::stri_enc_isascii(file_name)

  name_parts <- strsplit(file_name, "_", fixed = TRUE)
  year_pattern <- get_pipeline_constants()$patterns$yearbook_token_4digit
  # extract yearbook and product in a single pass over name_parts to avoid

  # iterating twice (mirrors extract_yearbook / extract_product logic inline)
  metadata_pairs <- vapply(
    name_parts,
    function(parts) {
      year_token_idx <- which(grepl(year_pattern, parts))[1]
      yb <- if (length(parts) >= 2 && !is.na(year_token_idx)) {
        paste(parts[2], parts[year_token_idx], sep = "_")
      } else {
        NA_character_
      }
      pr <- if (length(parts) > 6) {
        product_parts <- parts[7:length(parts)]
        product_parts[length(product_parts)] <- fs::path_ext_remove(
          product_parts[length(product_parts)]
        )
        paste(product_parts, collapse = "_")
      } else {
        NA_character_
      }
      c(yb, pr)
    },
    character(2)
  )
  yearbook <- metadata_pairs[1L, ]
  product <- metadata_pairs[2L, ]

  metadata <- data.table::data.table(
    file_path = as.character(file_paths),
    file_name = file_name,
    product = product,
    yearbook = yearbook,
    is_ascii = is_ascii,
    error_message = ifelse(
      !is_ascii,
      paste0("non-ascii file name detected: ", file_name),
      NA_character_
    )
  )

  return(metadata)
}

#' @title discover files
#' @description recursively discover all xlsx files inside an import directory,
#' validate the directory input, and return standardized metadata for each file.
#' when no files are found, the function emits a cli warning and returns an empty
#' metadata data.table with stable column names.
#' @param import_folder character scalar path to an existing directory.
#' @return a data.table with columns `file_path`, `file_name`, `product`,
#' `yearbook`, `is_ascii`, and `error_message`. returns zero rows when no xlsx
#' files are found.
#' @importFrom checkmate check_string check_directory_exists
#' @importFrom fs dir_ls
#' @importFrom cli cli_warn
#' @examples
#' temp_import_folder <- tempfile("import_")
#' fs::dir_create(temp_import_folder)
#' fs::file_create(file.path(temp_import_folder, "crops_2020_sample.xlsx"))
#' discover_files(temp_import_folder)
discover_files <- function(import_folder) {
  assert_or_abort(checkmate::check_string(import_folder, min.chars = 1))
  assert_or_abort(checkmate::check_directory_exists(import_folder))

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

    return(build_empty_file_metadata())
  }

  metadata <- extract_file_metadata(files_found)

  return(metadata)
}

#' @title discover pipeline files
#' @description retrieve the raw import folder from a pipeline configuration list,
#' validate configuration structure and target path, and delegate file discovery to
#' `discover_files`.
#' @param config named list containing `paths$data$import$raw` as a character
#' scalar path to an existing directory.
#' @return a data.table with discovered file metadata from `discover_files`.
#' @importFrom checkmate check_list check_string check_directory_exists
#' @importFrom cli cli_abort
#' @examples
#' temp_import_folder <- tempfile("import_")
#' fs::dir_create(temp_import_folder)
#' config_example <- list(paths = list(data = list(import = list(raw = temp_import_folder))))
#' discover_pipeline_files(config_example)
discover_pipeline_files <- function(config) {
  assert_or_abort(checkmate::check_list(config, any.missing = FALSE))

  import_folder <- config[["paths"]][["data"]][["import"]][["raw"]]

  if (is.null(import_folder)) {
    cli::cli_abort("`config$paths$data$import$raw` must be defined.")
  }

  assert_or_abort(checkmate::check_string(import_folder, min.chars = 1))
  assert_or_abort(checkmate::check_directory_exists(import_folder))

  metadata <- discover_files(import_folder)

  return(metadata)
}
