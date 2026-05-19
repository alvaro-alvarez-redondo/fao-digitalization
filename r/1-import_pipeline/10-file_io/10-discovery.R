# discovery helpers for import pipeline

#' Discover Excel files in import folder
#' Recursively searches an import folder for `.xlsx` files and returns metadata
#' for each file found. Warns and returns empty metadata when no files are found.
#' @param import_folder Character scalar path to the import directory.
#' @return `data.table` with columns `file_path`, `file_name`, `commodity`,
#'   `yearbook`, `is_ascii`, and `error_message`.
#' @examples
#' \dontrun{
#' discover_files("data/1-import/10-raw_import")
#' }
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

#' Resolve import folder from config and discover files
#' Extracts the raw import folder path from `config` and delegates to
#' `discover_files()` to perform the actual file discovery.
#' @param config Named configuration list containing `paths$data$import$raw`.
#' @return `data.table` of file metadata as returned by `discover_files()`.
#' @examples
#' \dontrun{
#' discover_pipeline_files(config)
#' }
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
