# script: helpers script
# description: provides helper functions for validation, string normalization,
# export path generation, and data.table coercion used across the pipeline

#' @title Format elapsed time as a human-readable string
#' @description Converts elapsed seconds into a concise label suitable for CLI
#'   messages. Returns seconds, minutes+seconds, or hours+minutes depending on
#'   the magnitude.
#' @param elapsed_seconds Numeric scalar of non-negative elapsed seconds.
#' @return Character scalar formatted elapsed time.
#' @importFrom checkmate assert_number
#' @examples
#' format_elapsed_time(0.5)
#' format_elapsed_time(75)
#' format_elapsed_time(3661)
format_elapsed_time <- function(elapsed_seconds) {
  checkmate::assert_number(elapsed_seconds, lower = 0, finite = TRUE)

  if (elapsed_seconds < 60) {
    return(sprintf("%.1fs", elapsed_seconds))
  }

  total_seconds <- as.integer(round(elapsed_seconds))
  hours <- total_seconds %/% 3600L
  minutes <- (total_seconds %% 3600L) %/% 60L
  seconds <- total_seconds %% 60L

  if (hours > 0L) {
    return(sprintf("%dh %dm", hours, minutes))
  }

  return(sprintf("%dm %ds", minutes, seconds))
}


#' @title assert checkmate validation results with cli errors
#' @description lightweight wrapper that checks the output of a
#' `checkmate::check_*` call. when the check returns a character error message
#' (i.e. validation failed), the function aborts with a structured cli error.
#' passes through `TRUE` results with minimal overhead.
#' @param check_result logical `TRUE` or character error string returned by a
#' `checkmate::check_*` function.
#' @return invisible `TRUE` when validation succeeds.
#' @importFrom cli cli_abort
#' @examples
#' assert_or_abort(checkmate::check_string("ok"))
assert_or_abort <- function(check_result) {
  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }
  return(invisible(TRUE))
}

# --- string normalization -----------------------------------------------------
# NOTE ON janitor COMPATIBILITY
#
# These three functions intentionally do NOT use `janitor::make_clean_names()`
# or `janitor::clean_names()`, even though janitor is available in the project.
# The reasons are:
#
# 1. OUTPUT FORMAT — janitor::make_clean_names() produces snake_case identifiers
#    separated by underscores (e.g. "food balance" → "food_balance").
#    normalize_string() intentionally keeps **space** separators because its
#    output is used as a match-key encoder throughout the pipeline: in
#    encode_rule_match_key() (rule engine), normalize_key_fields() (import
#    transform), and product/unit key joins in standardize_units. Changing to
#    underscores would silently break all of these joins.
#
# 2. DIGIT-STARTING STRINGS — janitor::make_clean_names() prepends an "x" to
#    any token that starts with a digit or underscore so that the result is a
#    valid R identifier (e.g. "2020" → "x2020"). normalize_string() preserves
#    digit-starting strings as-is. Applying make_clean_names() would corrupt
#    year values, numeric unit codes, and any rule value that starts with a
#    digit, causing silent lookup misses.
#
# 3. TARGET AUDIENCE — janitor::clean_names() operates on column **names** of a
#    data.frame, not on arbitrary character vectors of data values.
#    normalize_string_impl() and normalize_string() operate on row-level text
#    content, which is a fundamentally different use case.
#
# 4. PERFORMANCE — normalize_string_impl() is a hot-path inner-loop function
#    called per-row during import transforms and rule-engine joins. It
#    deliberately skips input validation. Routing through janitor would add
#    dispatch overhead not acceptable in this context.
#
# 5. FALLBACK SEMANTICS — normalize_filename() replaces NA and empty-string
#    results with the sentinel value "unknown". janitor has no equivalent
#    fallback for missing or empty inputs.

#' @title fast internal string normalization
#' @description converts an atomic vector to lowercase ascii, removes
#' non-alphanumeric characters except spaces, and squishes repeated spaces.
#' skips input validation for performance in hot paths. callers must ensure
#' the input is a valid atomic vector.
#'
#' **why not `janitor::make_clean_names()`?**
#' (1) janitor produces underscore-separated snake_case; this function keeps
#' spaces so downstream joins via `encode_rule_match_key()` stay consistent.
#' (2) janitor prepends `x` to digit-starting tokens (`"2020"` → `"x2020"`),
#' which would corrupt year values and numeric lookup keys.
#' (3) janitor targets column names, not row-level text values.
#' @param x atomic vector to normalize. coerced to character internally.
#' @return character vector with normalized lowercase ascii text, using single
#' spaces as word separators. `NA` inputs produce `NA` outputs.
#' @importFrom stringi stri_trans_general stri_replace_all_regex stri_trim_both
normalize_string_impl <- function(x) {
  out <- stringi::stri_trans_general(x, "Latin-ASCII; Lower")
  out <- stringi::stri_replace_all_regex(out, "[^a-z0-9]+", " ")
  stringi::stri_trim_both(out)
}

#' @title normalize free text into lowercase ascii
#' @description converts input text to lowercase ascii, removes non-alphanumeric
#' characters except spaces, and squishes repeated spaces to one separator.
#'
#' **why not `janitor::make_clean_names()`?**
#' (1) janitor produces underscore-separated snake_case; this function keeps
#' spaces so all callers (`encode_rule_match_key()`, `normalize_key_fields()`,
#' unit/product key joins) produce consistent space-separated match keys.
#' (2) janitor prepends `x` to digit-starting tokens (`"2020"` → `"x2020"`),
#' which would corrupt year values and numeric lookup keys.
#' (3) janitor targets column names, not row-level text values.
#' @param string atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector with normalized lowercase ascii text, using single
#' spaces as word separators. `NA` inputs produce `NA` outputs.
#' @importFrom checkmate check_atomic
#' @examples
#' normalize_string("forest! data 2024")
normalize_string <- function(string) {
  checkmate::assert_atomic_vector(
    string,
    min.len = 1,
    any.missing = TRUE
  )
  normalize_string_impl(string)
}

#' @title normalize file-friendly names
#' @description normalizes text and replaces spaces with underscores for
#' deterministic filename stems. missing and empty outputs are replaced by
#' `"unknown"`.
#'
#' **why not `janitor::make_clean_names()`?**
#' (1) janitor prepends `x` to digit-starting tokens (`"2020"` → `"x2020"`),
#' which would corrupt year-based filename stems and numeric product identifiers.
#' (2) janitor has no `"unknown"` fallback for `NA` or empty-string inputs.
#' (3) janitor targets R-identifier column names, not filesystem path stems.
#' @param filename atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector containing lowercase ascii filename stems with
#' underscores as word separators. `NA` and empty-string inputs return
#' `"unknown"`.
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
#' @description lightweight guard that converts a data.frame to a data.table
#' in place using `data.table::setDT()`, avoiding a full memory copy.
#' existing `data.table` inputs are returned immediately with no overhead.
#' callers are responsible for upstream validation; this function performs
#' only the minimal `is.data.table()` class check for speed.
#' @param df data.frame or data.table with zero or more rows.
#' @return data.table object.
#' @importFrom data.table is.data.table setDT
#' @examples
#' ensure_data_table(data.frame(x = 1:3))
ensure_data_table <- function(df) {
  if (!data.table::is.data.table(df)) {
    data.table::setDT(df)
  }
  return(df)
}

#' @title create an independent data.table copy
#' @description returns a deep copy of the input as a data.table. when the
#' input is already a data.table, only `data.table::copy()` is called. when
#' the input is a plain data.frame, `data.table::as.data.table()` already
#' allocates a fresh object, making the additional `copy()` unnecessary.
#' replaces the `copy(as.data.table(x))` double-allocation pattern.
#' callers are responsible for upstream validation.
#' @param df data.frame or data.table with zero or more rows.
#' @return a new data.table that can be modified by reference without affecting
#' the original.
#' @importFrom data.table as.data.table copy is.data.table
#' @examples
#' copy_as_data_table(data.frame(x = 1:3))
copy_as_data_table <- function(df) {
  if (data.table::is.data.table(df)) {
    return(data.table::copy(df))
  }
  return(data.table::as.data.table(df))
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
#' using folder and suffix metadata from the pipeline config. callers must
#' ensure the output directory exists before writing (see `run_export_pipeline`).
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

  checkpoint_path <- fs::path(
    here::here(),
    "data",
    ".checkpoints",
    paste0(checkpoint_name, ".rds")
  )

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


#' @title Drop rows where value column is NA
#' @description Removes rows from a `data.table` where the specified value
#' column is `NA`. Controlled by the `fao.drop_na_values` option (default
#' `TRUE`). When the option is `FALSE`, the data is returned unchanged.
#' @param dt `data.table` to filter.
#' @param value_column Character scalar column name to check for `NA` values.
#' @return Filtered `data.table` (copy when rows are dropped; original when
#' nothing changes or the toggle is off).
#' @importFrom checkmate assert_data_frame assert_string
drop_na_value_rows <- function(dt, value_column = "value") {
  checkmate::assert_data_frame(dt, min.rows = 0)
  checkmate::assert_string(value_column, min.chars = 1)

  if (!isTRUE(getOption("fao.drop_na_values", TRUE))) {
    return(dt)
  }

  if (!value_column %in% colnames(dt)) {
    return(dt)
  }

  keep_idx <- which(!is.na(dt[[value_column]]))

  if (length(keep_idx) == nrow(dt)) {
    return(dt)
  }

  return(dt[keep_idx, ])
}


#' @title Cached unzip with timestamp guard
#' @description Extracts a `.zip` archive only when the source archive is newer
#' than the target directory or when the target does not exist. This avoids
#' redundant `utils::unzip()` calls that dominate I/O profiling traces.
#' @param zip_path Character scalar path to the `.zip` file.
#' @param exdir Character scalar path to the extraction target directory.
#' @param overwrite Logical scalar; when `TRUE`, always extract regardless of
#' timestamps.
#' @return Invisible character scalar `exdir`.
#' @importFrom checkmate assert_string assert_flag assert_file_exists
#' @importFrom fs dir_exists dir_create file_info
#' @importFrom utils unzip
#' @importFrom cli cli_alert_info
#' @examples
#' # cached_unzip("data/archive.zip", "data/extracted")
cached_unzip <- function(zip_path, exdir, overwrite = FALSE) {
  checkmate::assert_string(zip_path, min.chars = 1)
  checkmate::assert_string(exdir, min.chars = 1)
  checkmate::assert_flag(overwrite)
  checkmate::assert_file_exists(zip_path, access = "r")

  needs_extract <- overwrite || !fs::dir_exists(exdir)

  if (!needs_extract) {
    zip_info <- fs::file_info(zip_path)
    exdir_info <- fs::file_info(exdir)
    needs_extract <- is.na(exdir_info$modification_time) ||
      zip_info$modification_time > exdir_info$modification_time
  }

  if (needs_extract) {
    fs::dir_create(exdir, recurse = TRUE)
    utils::unzip(zip_path, exdir = exdir, overwrite = TRUE)
    cli::cli_alert_info("Extracted {.file {zip_path}} to {.path {exdir}}")
  }

  return(invisible(exdir))
}
