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

#' @title fast internal string normalization
#' @description converts an atomic vector to lowercase ascii, removes
#' non-alphanumeric characters except spaces, and squishes repeated spaces.
#' skips input validation for performance in hot paths. callers must ensure
#' the input is a valid atomic vector.
#' @param x atomic vector to normalize. coerced to character internally.
#' @return character vector with normalize lowercase ascii text.
#' @importFrom stringi stri_trans_general stri_replace_all_regex stri_trim_both
#' @importFrom data.table uniqueN
normalize_string_impl <- function(x) {
  constants <- get_pipeline_constants()
  non_alnum_pattern <- constants$patterns$normalize_non_alnum
  normalize_pattern <- constants$patterns$normalize_already_clean

  values_chr <- as.character(x)
  non_na_idx <- !is.na(values_chr)

  if (!any(non_na_idx)) {
    return(values_chr)
  }

  values_non_na <- values_chr[non_na_idx]
  values_n <- length(values_non_na)

  use_unique_path <- FALSE
  perf_cfg <- constants$performance
  if (values_n >= perf_cfg$normalize_unique_min_n) {
    sample_n <- min(values_n, perf_cfg$normalize_unique_sample_n)
    unique_ratio <- data.table::uniqueN(values_non_na[seq_len(sample_n)]) /
      sample_n
    use_unique_path <- unique_ratio <= perf_cfg$normalize_unique_ratio_threshold
  }

  if (isTRUE(use_unique_path)) {
    unique_values <- unique(values_non_na)
    if (all(stringi::stri_detect_regex(unique_values, normalize_pattern))) {
      values_chr[non_na_idx] <- unique_values[match(
        values_non_na,
        unique_values
      )]
      return(values_chr)
    }

    normalize_unique <- stringi::stri_trans_general(
      unique_values,
      "Latin-ASCII; Lower"
    )
    normalize_unique <- stringi::stri_replace_all_regex(
      normalize_unique,
      non_alnum_pattern,
      " "
    )
    normalize_unique <- stringi::stri_trim_both(normalize_unique)
    values_chr[non_na_idx] <- normalize_unique[match(
      values_non_na,
      unique_values
    )]
    return(values_chr)
  }

  normalize_values <- stringi::stri_trans_general(
    values_non_na,
    "Latin-ASCII; Lower"
  )
  normalize_values <- stringi::stri_replace_all_regex(
    normalize_values,
    non_alnum_pattern,
    " "
  )
  values_chr[non_na_idx] <- stringi::stri_trim_both(normalize_values)

  return(values_chr)
}

#' @title normalize free text into lowercase ascii
#' @description converts input text to lowercase ascii, removes non-alphanumeric
#' characters except spaces, and squishes repeated spaces to one separator.
#' @param string atomic vector with length greater than or equal to one.
#' validated with `checkmate::check_atomic(min.len = 1, any.missing = true)`.
#' @return character vector with normalize lowercase ascii text.
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

#' @title fast internal footnote string normalization
#' @description converts footnote text to lowercase ascii and removes characters
#' that are not alphanumeric, spaces, or footnote-safe punctuation
#' (`;`, `/`, `*`, `(`, `)`, `.`, `,`, `-`, `#`, `%`, `:`).
#' unlike `normalize_string_impl`, this preserves the special characters
#' commonly found in footnotes (reference markers, separators, parenthetical
#' labels). skips input validation for performance in hot paths.
#' @param x atomic vector to normalize. coerced to character internally.
#' @return character vector with normalize lowercase ascii footnote text.
#' @importFrom stringi stri_trans_general stri_replace_all_regex stri_trim_both
clean_footnote_impl <- function(x) {
  out <- stringi::stri_trans_general(x, "Latin-ASCII; Lower")
  out <- stringi::stri_replace_all_regex(out, "[^a-z0-9 ;/*()\\.\\-,#%:]+", " ")
  stringi::stri_trim_both(out)
}

#' @title normalize footnote text into lowercase ascii
#' @description converts footnote text to lowercase ascii, removing characters
#' that are not alphanumeric, spaces, or common footnote punctuation while
#' preserving reference markers (`;`, `/`, `*`, `(`, `)`, `.`, `,`, `-`, `#`,
#' `%`, `:`). use this instead of `normalize_string()` for footnotes columns to
#' avoid stripping meaningful symbols.
#' @param x atomic vector with length greater than or equal to one. validated
#' with `checkmate::assert_atomic_vector(min.len = 1, any.missing = TRUE)`.
#' @return character vector with normalize lowercase ascii footnote text.
#' @importFrom checkmate assert_atomic_vector
#' @examples
#' clean_footnote("Note 1/ Official data (revised); 50% estimate")
clean_footnote <- function(x) {
  checkmate::assert_atomic_vector(x, min.len = 1, any.missing = TRUE)
  clean_footnote_impl(x)
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

  normalize_filename <- filename |>
    normalize_string() |>
    stringr::str_replace_all(" ", "_")

  normalize_filename[
    is.na(normalize_filename) | normalize_filename == ""
  ] <- "unknown"

  return(normalize_filename)
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

  if (
    (is.double(x) || is.integer(x)) &&
      is.null(attr(x, "class", exact = TRUE))
  ) {
    return(as.numeric(x))
  }

  if (is.character(x)) {
    return(suppressWarnings(as.numeric(x)))
  }

  return(suppressWarnings(as.numeric(as.character(x))))
}

#' @title extract yearbook token from parsed name parts
#' @description extracts token 2 and the first token matching a 4-digit year
#' from a parsed filename token vector, and joins them with an underscore.
#' @param parts character vector with no missing values and length greater than
#' or equal to one. validated with
#' `checkmate::check_character(min.len = 1, any.missing = false)`.
#' @return character scalar with combined yearbook tokens, or `NA_character_`
#' when token 2 is missing or no 4-digit year token is present.
#' @importFrom checkmate check_character
#' @examples
#' extract_yearbook(c("whep", "yb", "2020", "2021", "file.xlsx"))
extract_yearbook <- function(parts) {
  assert_or_abort(checkmate::check_character(
    parts,
    min.len = 1,
    any.missing = FALSE
  ))

  if (length(parts) < 2) {
    return(NA_character_)
  }

  year_pattern <- get_pipeline_constants()$patterns$yearbook_token_4digit
  year_token_idx <- which(grepl(year_pattern, parts))[1]

  if (is.na(year_token_idx)) {
    return(NA_character_)
  }

  return(paste(parts[2], parts[year_token_idx], sep = "_"))
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
#' config <- list(paths = list(data = list(export = list(processed = "tmp"))))
#' get_config_string(config, c("paths", "data", "export", "processed"), "field")
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

#' @title build normalize export path from pipeline config
#' @description constructs an output path for `processed` or `lists` export
#' using folder and suffix metadata from the pipeline config. callers must
#' ensure the output directory exists before writing (see `run_export_pipeline`).
#' @param config named list with non-empty structure. validated with
#' `checkmate::check_list(min.len = 1)`. must contain
#' `paths$data$export$processed`, `paths$data$export$lists`,
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
#'   paths = list(data = list(export = list(processed = "tmp", lists = "tmp"))),
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
      path = c("paths", "data", "export", "processed"),
      field_name = "config$paths$data$export$processed"
    ),
    lists = get_config_string(
      config = config,
      path = c("paths", "data", "export", "lists"),
      field_name = "config$paths$data$export$lists"
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
#' emitted. defaults to `getOption("whep.progress.enabled", TRUE)`.
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
  enable_progress = getOption("whep.progress.enabled", TRUE)
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


#' @title Sort pipeline stage data deterministically
#' @description Applies deterministic row sorting for pipeline stage outputs
#' using the canonical business key order:
#' `hemisphere`, `continent`, `country`, `product`, `variable`, `unit`,
#' `year`, `value`, `notes`, `footnotes`, `yearbook`, and `document`.
#' Missing sort columns are ignored, and sorting is skipped when none are
#' present.
#' @param dt Data frame or `data.table` to sort.
#' @param sort_columns Character vector sort priority.
#' @return `data.table` sorted in place when possible.
#' @importFrom checkmate check_character
#' @examples
#' sort_pipeline_stage_dt(data.table::data.table(country = "x", year = "2020"))
sort_pipeline_stage_dt <- function(
  dt,
  sort_columns = get_pipeline_constants()$sorting$stage_row_order
) {
  sorted_dt <- coerce_to_data_table(dt)
  assert_or_abort(checkmate::check_character(
    sort_columns,
    any.missing = FALSE,
    min.len = 1
  ))

  present_sort_columns <- intersect(sort_columns, names(sorted_dt))

  if (length(present_sort_columns) == 0L || nrow(sorted_dt) <= 1L) {
    return(sorted_dt)
  }

  data.table::setorderv(sorted_dt, present_sort_columns, na.last = TRUE)

  return(sorted_dt)
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

  if (!isTRUE(getOption("whep.checkpointing.enabled", FALSE))) {
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

  if (!isTRUE(getOption("whep.checkpointing.enabled", FALSE))) {
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
#' column is `NA`. Controlled by the `whep.drop_na_values` option (default
#' `TRUE`). When the option is `FALSE`, the data is returned unchanged.
#' @param dt `data.table` to filter.
#' @param value_column Character scalar column name to check for `NA` values.
#' @return Filtered `data.table` (copy when rows are dropped; original when
#' nothing changes or the toggle is off).
#' @importFrom checkmate assert_string
drop_na_value_rows <- function(dt, value_column = "value") {
  if (!(data.table::is.data.table(dt) || is.data.frame(dt))) {
    cli::cli_abort("{.arg dt} must be a data.frame or data.table")
  }
  checkmate::assert_string(value_column, min.chars = 1)

  drop_na_option <- get_pipeline_constants()$toggle_options$drop_na_values
  if (!isTRUE(getOption(drop_na_option, TRUE))) {
    return(dt)
  }

  if (!value_column %in% names(dt)) {
    return(dt)
  }

  value_vec <- dt[[value_column]]

  if (data.table::is.data.table(dt)) {
    if (!anyNA(value_vec)) {
      return(dt)
    }

    return(dt[!is.na(value_vec)])
  }

  if (!anyNA(value_vec)) {
    return(dt)
  }

  return(dt[!is.na(value_vec), , drop = FALSE])
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
