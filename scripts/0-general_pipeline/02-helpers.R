# script: helpers script
# description: provides helper functions for validation, string normalization,
# export path generation, and data.table coercion used across the pipeline

#' @title assert checkmate validation results with cli errors
#' @description validates the output of a `checkmate::check_*` call, requiring a
#' `true` logical scalar or a non-empty error string. when validation fails, the
#' function aborts with a structured cli error message.
#' @param check_result logical true scalar or non-empty character scalar returned
#' by a `checkmate::check_*` function.
#' @return invisible logical true scalar when validation succeeds.
#' @importFrom checkmate assert check_string check_true
#' @importFrom cli cli_abort
#' @examples
#' assert_or_abort(checkmate::check_string("ok"))
assert_or_abort <- function(check_result) {
  checkmate::assert(
    checkmate::check_true(check_result),
    checkmate::check_string(check_result, min.chars = 1)
  )

  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }

  return(invisible(TRUE))
}

#' @title normalize free text into lowercase ascii
#' @description converts input text to lowercase ascii, removes non-alphanumeric
#' characters except spaces, and squishes repeated spaces to one separator.
#' @param string atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector with normalized lowercase ascii text.
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all str_squish str_to_lower
#' @importFrom stringi stri_trans_general
#' @examples
#' normalize_string("forest! data 2024")
normalize_string <- function(string) {
  assert_or_abort(checkmate::check_atomic(
    string,
    min.len = 1,
    any.missing = TRUE
  ))

  normalized_string <- as.character(string)
  normalized_string <- tolower(normalized_string)
  normalized_string <- stringi::stri_trans_general(normalized_string, "latin-ascii")
  normalized_string <- gsub("[^a-z0-9 ]", " ", normalized_string)
  normalized_string <- trimws(gsub("\\s+", " ", normalized_string))

  return(normalized_string)
}

#' @title normalize file-friendly names
#' @description normalizes text and replaces spaces with underscores for
#' deterministic filename stems. missing and empty outputs are replaced by
#' `"unknown"`.
#' @param filename atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector containing lowercase ascii filename stems.
#' @importFrom checkmate check_atomic
#' @importFrom stringr str_replace_all
#' @examples
#' normalize_filename("food balance sheet")
normalize_filename <- function(filename) {
  assert_or_abort(checkmate::check_atomic(
    filename,
    min.len = 1,
    any.missing = TRUE
  ))

  normalized_filename <- filename |>
    normalize_string() |>
    stringr::str_replace_all(" ", "_")

  normalized_filename[
    is.na(normalized_filename) | normalized_filename == ""
  ] <- "unknown"

  return(normalized_filename)
}

#' @title coerce vector values to numeric safely
#' @description converts an atomic vector to numeric using a stable character
#' intermediary. empty strings are treated as missing values and non-numeric
#' values are converted to `NA_real_` without raising warnings.
#' @param x atomic vector with length greater than or equal to one. validated
#' with `checkmate::check_atomic(min.len = 1, any.missing = TRUE)`.
#' @return numeric vector with the same length as `x`.
#' @importFrom checkmate check_atomic
#' @examples
#' coerce_numeric_safe(c("1", " 2.5 ", "", "abc"))
coerce_numeric_safe <- function(x) {
  assert_or_abort(checkmate::check_atomic(
    x,
    min.len = 1,
    any.missing = TRUE
  ))

  x_chr <- as.character(x)
  x_chr <- trimws(x_chr)
  x_chr[x_chr == ""] <- NA_character_

  return(suppressWarnings(as.numeric(x_chr)))
}

#' @title extract yearbook token from parsed name parts
#' @description extracts tokens in positions two through four from a parsed
#' filename token vector and joins them with underscores.
#' @param parts character vector with no missing values and length greater than
#' or equal to one. validated with
#' `checkmate::check_character(min.len = 1, any.missing = false)`.
#' @return character scalar with combined yearbook tokens, or `NA_character_`
#' when the input has fewer than four elements.
#' @importFrom checkmate check_character
#' @examples
#' extract_yearbook(c("fao", "yb", "2020", "2021", "file.xlsx"))
extract_yearbook <- function(parts) {
  assert_or_abort(checkmate::check_character(
    parts,
    min.len = 1,
    any.missing = FALSE
  ))

  if (length(parts) >= 4) {
    return(paste(parts[2:4], collapse = "_"))
  }

  return(NA_character_)
}

#' @title extract product token suffix from parsed name parts
#' @description extracts tokens from index seven onward, removes the file
#' extension from the final token, and joins the result with underscores.
#' @param parts character vector with no missing values and length greater than
#' or equal to one. validated with
#' `checkmate::check_character(min.len = 1, any.missing = false)`.
#' @return character scalar with product tokens, or `NA_character_` when the
#' input has fewer than seven elements.
#' @importFrom checkmate check_character
#' @importFrom fs path_ext_remove
#' @examples
#' extract_product(c("a", "b", "c", "d", "e", "f", "rice", "grain.xlsx"))
extract_product <- function(parts) {
  assert_or_abort(checkmate::check_character(
    parts,
    min.len = 1,
    any.missing = FALSE
  ))

  if (length(parts) > 6) {
    product_parts <- parts[7:length(parts)]
    product_parts[length(product_parts)] <- fs::path_ext_remove(
      product_parts[length(product_parts)]
    )

    return(paste(product_parts, collapse = "_"))
  }

  return(NA_character_)
}

#' @title ensure data.frame input is a data.table
#' @description validates a data.frame-compatible input and returns a
#' `data.table`, preserving existing `data.table` inputs unchanged.
#' @param df data.frame or data.table with zero or more rows. validated with
#' `checkmate::check_data_frame(min.rows = 0)`.
#' @return data.table object.
#' @importFrom checkmate check_data_frame
#' @importFrom data.table as.data.table is.data.table
#' @examples
#' ensure_data_table(data.frame(x = 1:3))
ensure_data_table <- function(df) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 0))

  if (!data.table::is.data.table(df)) {
    return(data.table::as.data.table(df))
  }

  return(df)
}

#' @title validate export-ready import data
#' @description validates export inputs and returns a data.table for stable
#' downstream export operations.
#' @param df data.frame or data.table with at least one row. validated with
#' `checkmate::check_data_frame(min.rows = 1)`.
#' @param base_name non-empty character scalar. validated with
#' `checkmate::check_string(min.chars = 1)`.
#' @return data.table with at least one row.
#' @importFrom checkmate check_data_frame check_string
#' @examples
#' validate_export_import(data.frame(x = 1), "dataset")
validate_export_import <- function(df, base_name) {
  assert_or_abort(checkmate::check_data_frame(df, min.rows = 1))
  assert_or_abort(checkmate::check_string(base_name, min.chars = 1))

  return(ensure_data_table(df))
}

#' @title extract and validate string field from nested config list
#' @description retrieves a nested field from `config` using `purrr::pluck()`,
#' aborts with a cli error when the field is missing, and validates that the
#' retrieved value is a non-empty character scalar.
#' @param config named list containing pipeline settings.
#' @param path character vector that defines the nested access path.
#' @param field_name character scalar used in validation error messages.
#' @return non-empty character scalar extracted from the config list.
#' @importFrom purrr pluck
#' @importFrom checkmate check_list check_character check_string
#' @importFrom cli cli_abort
#' @examples
#' config <- list(paths = list(data = list(exports = list(processed = "tmp"))))
#' get_config_string(config, c("paths", "data", "exports", "processed"), "field")
get_config_string <- function(config, path, field_name) {
  assert_or_abort(checkmate::check_list(config, min.len = 1))
  assert_or_abort(checkmate::check_character(
    path,
    min.len = 1,
    any.missing = FALSE
  ))
  assert_or_abort(checkmate::check_string(field_name, min.chars = 1))

  field_value <- purrr::pluck(config, !!!path, .default = NULL)

  if (is.null(field_value)) {
    cli::cli_abort("`{field_name}` must be defined.")
  }

  assert_or_abort(checkmate::check_string(field_value, min.chars = 1))

  return(field_value)
}

#' @title build normalized export path from pipeline config
#' @description constructs an output path for `processed` or `lists` exports
#' using folder and suffix metadata from the pipeline config.
#' @param config named list with non-empty structure. validated with
#' `checkmate::check_list(min.len = 1)`. must contain
#' `paths$data$exports$processed`, `paths$data$exports$lists`,
#' `export_config$data_suffix`, and `export_config$list_suffix` as non-empty
#' character scalars.
#' @param base_name non-empty character scalar used as output basename before
#' normalization and suffix append. validated with
#' `checkmate::check_string(min.chars = 1)`.
#' @param type character scalar. one of `"processed"` or `"lists"`.
#' @param use_here logical scalar. when `true`, resolve export directories
#' with `here::here()` to guarantee project-root-relative output paths.
#' @return character scalar path generated with `fs::path()`.
#' @importFrom checkmate check_flag check_list check_string
#' @importFrom fs path
#' @importFrom here here
#' @examples
#' config <- list(
#'   paths = list(data = list(exports = list(processed = "tmp", lists = "tmp"))),
#'   export_config = list(data_suffix = "_data.xlsx", list_suffix = "_list.xlsx")
#' )
#' generate_export_path(config, "food balance", "processed")
generate_export_path <- function(
  config,
  base_name,
  type = c("processed", "lists"),
  use_here = TRUE
) {
  assert_or_abort(checkmate::check_list(config, min.len = 1))
  assert_or_abort(checkmate::check_string(base_name, min.chars = 1))
  assert_or_abort(checkmate::check_flag(use_here))

  type <- match.arg(type)

  folder <- switch(
    type,
    processed = get_config_string(
      config = config,
      path = c("paths", "data", "exports", "processed"),
      field_name = "config$paths$data$exports$processed"
    ),
    lists = get_config_string(
      config = config,
      path = c("paths", "data", "exports", "lists"),
      field_name = "config$paths$data$exports$lists"
    )
  )

  suffix <- switch(
    type,
    processed = get_config_string(
      config = config,
      path = c("export_config", "data_suffix"),
      field_name = "config$export_config$data_suffix"
    ),
    lists = get_config_string(
      config = config,
      path = c("export_config", "list_suffix"),
      field_name = "config$export_config$list_suffix"
    )
  )

  output_folder <- if (use_here) {
    here::here(folder)
  } else {
    folder
  }

  ensure_directories_exist(output_folder, recurse = TRUE)

  return(fs::path(output_folder, paste0(normalize_filename(base_name), suffix)))
}


#' @title map with optional progressr reporting
#' @description applies a function over an input vector with optional progress
#' updates powered by `progressr`. the helper respects global progressr
#' configuration and can be disabled via argument or option.
#' @param x vector or list to iterate over.
#' @param .f function applied to each element of `x`.
#' @param ... additional arguments passed to `.f`.
#' @param message_template optional character scalar format string passed to
#' `sprintf()`. supports `%d` placeholders for current index and total steps.
#' @param message_fn optional function with signature
#' `function(item, index, total_steps)` returning a character scalar progress
#' message.
#' @param enable_progress logical scalar indicating whether progress updates are
#' emitted. defaults to `getOption("fao.progress.enabled", TRUE)`.
#' @return list with one element per input item, matching `purrr::map()`
#' semantics.
#' @importFrom checkmate check_atomic_vector check_flag check_function check_list check_string
#' @importFrom progressr progressor with_progress
#' @importFrom purrr imap map
#' @examples
#' map_with_progress(1:3, \(x) x * 2, enable_progress = FALSE)
map_with_progress <- function(
  x,
  .f,
  ...,
  message_template = NULL,
  message_fn = NULL,
  enable_progress = getOption("fao.progress.enabled", TRUE)
) {
  list_check_result <- checkmate::check_list(x, min.len = 0, any.missing = TRUE)
  atomic_check_result <- checkmate::check_atomic_vector(
    x,
    min.len = 0,
    any.missing = TRUE
  )

  input_check_result <- if (isTRUE(list_check_result)) {
    TRUE
  } else {
    atomic_check_result
  }

  assert_or_abort(input_check_result)
  assert_or_abort(checkmate::check_function(.f))
  assert_or_abort(checkmate::check_flag(enable_progress))

  if (!is.null(message_template)) {
    assert_or_abort(checkmate::check_string(message_template, min.chars = 1))
  }

  if (!is.null(message_fn)) {
    assert_or_abort(checkmate::check_function(message_fn))
  }

  total_steps <- length(x)

  if (!enable_progress || total_steps == 0) {
    return(purrr::map(x, \(item) .f(item, ...)))
  }

  resolve_progress_message <- function(item, index, total_steps) {
    progress_message <- NULL

    if (!is.null(message_fn)) {
      progress_message <- message_fn(item, index, total_steps)
    } else if (!is.null(message_template)) {
      progress_message <- sprintf(message_template, index, total_steps)
    }

    if (is.null(progress_message)) {
      return(NULL)
    }

    assert_or_abort(checkmate::check_string(progress_message, min.chars = 1))

    return(progress_message)
  }

  return(progressr::with_progress({
    progress <- progressr::progressor(steps = total_steps)

    purrr::imap(x, \(item, index) {
      progress_message <- resolve_progress_message(item, index, total_steps)

      if (is.null(progress_message)) {
        progress()
      } else {
        progress(progress_message)
      }

      .f(item, ...)
    })
  }))
}

#' @title coerce to data.table
#' @description validate a data.frame-compatible object and return a
#' `data.table`, preserving `data.table` inputs.
#' @param x data.frame or data.table object.
#' @param min_rows non-negative integer scalar for minimum row requirement.
#' @return data.table.
#' @importFrom checkmate check_data_frame check_int
#' @examples
#' coerce_to_data_table(data.frame(x = 1:2))
coerce_to_data_table <- function(x, min_rows = 0L) {
  assert_or_abort(checkmate::check_int(min_rows, lower = 0))
  assert_or_abort(checkmate::check_data_frame(x, min.rows = min_rows))

  return(ensure_data_table(x))
}


#' @title Assign named values into an environment
#' @description Validates a named list and assigns each element to `env` using
#' deterministic one-by-one writes.
#' @param values Named list of objects to assign.
#' @param env Environment receiving assigned values.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_environment assert_list
#' @importFrom purrr iwalk
#' @examples
#' temp_env <- new.env(parent = emptyenv())
#' assign_environment_values(list(answer = 42L), temp_env)
assign_environment_values <- function(values, env) {
  assert_or_abort(checkmate::check_list(
    values,
    names = "named",
    any.missing = TRUE
  ))
  assert_or_abort(checkmate::check_environment(env))

  purrr::iwalk(values, \(value, object_name) {
    assign(object_name, value, envir = env)
  })

  return(invisible(TRUE))
}


#' @title Save pipeline checkpoint to disk
#' @description Serializes a pipeline result to an RDS file for crash recovery
#' and resumption of long-running pipeline stages. When checkpointing is
#' disabled via options, this function silently returns `NULL`.
#' @param result Object to serialize.
#' @param checkpoint_name Character scalar checkpoint identifier.
#' @param config Named configuration list with `paths$data` containing the
#' project data root.
#' @return Character scalar path to the checkpoint file, or `NULL` when
#' checkpointing is disabled.
#' @importFrom checkmate check_string check_list
#' @importFrom fs path dir_create
#' @importFrom cli cli_alert_info
save_pipeline_checkpoint <- function(result, checkpoint_name, config) {
  assert_or_abort(checkmate::check_string(checkpoint_name, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, min.len = 1))

  if (!isTRUE(getOption("fao.checkpointing.enabled", FALSE))) {
    return(invisible(NULL))
  }

  checkpoint_dir <- fs::path(here::here(), "data", ".checkpoints")
  fs::dir_create(checkpoint_dir, recurse = TRUE)

  checkpoint_path <- fs::path(checkpoint_dir, paste0(checkpoint_name, ".rds"))
  # compress = FALSE prioritizes speed over disk space, appropriate for
  # checkpoint files that are temporary and frequently overwritten.
  saveRDS(result, file = checkpoint_path, compress = FALSE)

  cli::cli_alert_info("Checkpoint saved: {.file {checkpoint_path}}")

  return(checkpoint_path)
}


#' @title Load pipeline checkpoint from disk
#' @description Attempts to load a previously saved checkpoint. Returns `NULL`
#' when no checkpoint file exists or checkpointing is disabled.
#' @param checkpoint_name Character scalar checkpoint identifier.
#' @param config Named configuration list.
#' @return Deserialized checkpoint object, or `NULL`.
#' @importFrom checkmate check_string check_list
#' @importFrom fs path file_exists
#' @importFrom cli cli_alert_success
load_pipeline_checkpoint <- function(checkpoint_name, config) {
  assert_or_abort(checkmate::check_string(checkpoint_name, min.chars = 1))
  assert_or_abort(checkmate::check_list(config, min.len = 1))

  if (!isTRUE(getOption("fao.checkpointing.enabled", FALSE))) {
    return(NULL)
  }

  checkpoint_path <- fs::path(here::here(), "data", ".checkpoints", paste0(checkpoint_name, ".rds"))

  if (!fs::file_exists(checkpoint_path)) {
    return(NULL)
  }

  result <- readRDS(checkpoint_path)
  cli::cli_alert_success("Checkpoint restored: {.file {checkpoint_path}}")

  return(result)
}


#' @title Clear pipeline checkpoints
#' @description Removes checkpoint directory and all saved checkpoints.
#' @param config Named configuration list.
#' @return Invisible `TRUE`.
#' @importFrom checkmate check_list
#' @importFrom fs path dir_exists dir_delete
#' @importFrom cli cli_alert_info
clear_pipeline_checkpoints <- function(config) {
  assert_or_abort(checkmate::check_list(config, min.len = 1))

  checkpoint_dir <- fs::path(here::here(), "data", ".checkpoints")

  if (fs::dir_exists(checkpoint_dir)) {
    fs::dir_delete(checkpoint_dir)
    cli::cli_alert_info("Checkpoints cleared: {.file {checkpoint_dir}}")
  }

  return(invisible(TRUE))
}
