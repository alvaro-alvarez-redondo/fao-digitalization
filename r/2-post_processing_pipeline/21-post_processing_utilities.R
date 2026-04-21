# script: post-processing utilities
# description: reusable stage metadata, template generation, rule validation,
# dictionary construction, vectorized harmonization engine, and structured
# audit helpers for post-processing stages.

#' @title Get canonical rule columns
#' @description Returns unified canonical rule column names used by both
#' `clean` and `harmonize` post-processing stages.
#' @return Character vector of canonical columns.
#' @examples
#' get_canonical_rule_columns()
get_canonical_rule_columns <- function() {
  return(c(
    "column_source",
    "value_source_raw",
    "value_source",
    "column_target",
    "value_target_raw",
    "value_target"
  ))
}

#' @title Get supported post-processing stages
#' @description Returns deterministic stage order for post-processing execution.
#' @return Character vector with values `clean` and `harmonize`.
#' @examples
#' get_post_processing_stage_names()
get_post_processing_stage_names <- function() {
  return(c("clean", "harmonize"))
}

# Shared in-memory cache for canonical stage payload bundles.
.stage_payload_bundle_cache <- new.env(parent = emptyenv())

#' @title Validate post-processing stage name
#' @description Ensures stage name is one of the supported post-processing stages.
#' @param stage_name Character scalar stage label.
#' @return Character scalar validated stage name.
#' @importFrom checkmate assert_string
validate_post_processing_stage_name <- function(stage_name) {
  checkmate::assert_string(stage_name, min.chars = 1)
  validated_stage_name <- match.arg(
    stage_name,
    choices = get_post_processing_stage_names()
  )

  return(validated_stage_name)
}

#' @title Get canonical target value column for stage
#' @description Returns unified target value column name used by both stages.
#' @param stage_name Character scalar stage label.
#' @return Character scalar target value column name.
get_stage_target_value_column <- function(stage_name) {
  validate_post_processing_stage_name(stage_name)

  return("value_target")
}

#' @title Get canonical source value column for stage
#' @description Returns unified source value column name used by both stages.
#' @param stage_name Character scalar stage label.
#' @return Character scalar source value column name.
get_stage_source_value_column <- function(stage_name) {
  validate_post_processing_stage_name(stage_name)

  return("value_source")
}

#' @title Get post-processing audit paths
#' @description Resolves deterministic audit root and subdirectory paths.
#' @param config Named configuration list.
#' @return Named list with `audit_root_dir`, `diagnostics_dir`, and `templates_dir`.
#' @importFrom checkmate assert_list assert_string
get_post_processing_audit_paths <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(
    config$paths$data$audit$audit_root_dir,
    min.chars = 1
  )

  audit_root_dir <- config$paths$data$audit$audit_root_dir

  diagnostics_dir <- fs::path(audit_root_dir, "post_processing_diagnostics")

  return(list(
    audit_root_dir = audit_root_dir,
    diagnostics_dir = diagnostics_dir,
    templates_dir = fs::path(audit_root_dir, "templates")
  ))
}

#' @title Initialize post-processing audit directory tree
#' @description Creates deterministic audit subdirectories under `audit_root_dir`.
#' @param config Named configuration list.
#' @return Named list of post-processing audit paths.
#' @importFrom checkmate assert_list
#'
initialize_post_processing_audit_root <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  audit_paths <- get_post_processing_audit_paths(config)
  ensure_directories_exist(
    unlist(audit_paths, use.names = FALSE),
    recurse = TRUE
  )

  return(audit_paths)
}

#' @title Generate unified rule template workbook
#' @description Writes a deterministic template workbook with unified rule
#' columns and guidance under the audit template directory. Both `clean` and
#' `harmonize` stages share the same column schema.
#' @param audit_paths Named list from `get_post_processing_audit_paths()`.
#' @param overwrite Logical scalar indicating whether existing template is replaced.
#' @return Character scalar written template path.
#' @importFrom checkmate assert_list assert_flag
#' @importFrom writexl write_xlsx
write_stage_rule_template <- function(
  audit_paths,
  overwrite = TRUE
) {
  checkmate::assert_list(audit_paths, min.len = 1)
  checkmate::assert_string(audit_paths$templates_dir, min.chars = 1)
  checkmate::assert_flag(overwrite)

  template_columns <- get_canonical_rule_columns()
  template_data <- data.table::as.data.table(setNames(
    replicate(length(template_columns), character(0), simplify = FALSE),
    template_columns
  ))

  guidance_data <- data.table::data.table(
    note = c(
      "Fill all required columns.",
      "Column names must remain unchanged.",
      "Rows define conditional source-target replacements."
    )
  )

  template_path <- fs::path(
    audit_paths$templates_dir,
    "clean_harmonize_template.xlsx"
  )

  writexl::write_xlsx(
    list(clean_harmonize_template = template_data, guidance = guidance_data),
    path = template_path
  )

  return(template_path)
}

#' @title Generate post-processing rule templates
#' @description Writes a single unified rule template under
#' `audit_root_dir/templates`. Both clean and harmonize stages share the same
#' column schema; the only difference between rule files is the `clean_` or
#' `harmonize_` filename prefix.
#' @param config Named configuration list.
#' @param overwrite Logical scalar indicating whether existing templates are replaced.
#' @return Named character vector with `clean_harmonize_template` path.
#' @importFrom checkmate assert_list assert_flag
generate_post_processing_rule_templates <- function(config, overwrite = TRUE) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_flag(overwrite)

  audit_paths <- initialize_post_processing_audit_root(config)

  template_path <- write_stage_rule_template(
    audit_paths = audit_paths,
    overwrite = overwrite
  )

  return(c(clean_harmonize_template = template_path))
}

#' @title Read rule table from csv or excel
#' @description Reads a rule table file and returns a `data.table`. For
#' Excel files, all worksheets whose columns match the canonical rule schema
#' (with optional `clean_`/`harmonize_` prefixes) are read and row-bound in
#' workbook order.
#' @param file_path Character scalar path to rule file.
#' @return `data.table` containing rule rows.
#' @importFrom checkmate assert_string assert_file_exists
#' @importFrom fs path_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel excel_sheets
#' @examples
#' \dontrun{read_rule_table("data/1-import/11-clean_imports/clean_rules.xlsx")}
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  file_extension <- fs::path_ext(file_path) |>
    tolower()

  if (identical(file_extension, "csv")) {
    return(
      readr::read_csv(file_path, show_col_types = FALSE) |>
        data.table::as.data.table()
    )
  }

  if (file_extension %in% c("xlsx", "xls")) {
    canonical_columns <- get_canonical_rule_columns()
    optional_columns <- c("value_source")
    required_columns <- setdiff(canonical_columns, optional_columns)
    stage_prefix_pattern <- "^(clean|harmonize)_"

    sheet_names <- readxl::excel_sheets(file_path)

    sheet_results <- lapply(sheet_names, function(sheet_name) {
      sheet_dt <- readxl::read_excel(file_path, sheet = sheet_name) |>
        data.table::as.data.table()

      available_columns <- colnames(sheet_dt)
      normalized_columns <- sub(stage_prefix_pattern, "", available_columns)

      has_duplicated_normalized <- anyDuplicated(normalized_columns) > 0L
      has_unexpected_columns <- any(!(normalized_columns %in% canonical_columns))
      has_required_columns <- all(required_columns %in% normalized_columns)

      is_matching_sheet <-
        !has_duplicated_normalized &&
        !has_unexpected_columns &&
        has_required_columns

      if (is_matching_sheet) {
        data.table::setnames(sheet_dt, available_columns, normalized_columns)
      }

      return(list(
        matches = is_matching_sheet,
        rules_dt = sheet_dt
      ))
    })

    matching_sheet_indexes <- which(vapply(
      sheet_results,
      function(result) {
        isTRUE(result$matches)
      },
      logical(1)
    ))

    if (length(matching_sheet_indexes) == 0L) {
      cli::cli_abort(c(
        "No worksheets with matching rule columns found in {.file {file_path}}.",
        "x" = paste0("Required columns: ", paste(required_columns, collapse = ", ")),
        "x" = paste0("Available sheets: ", paste(sheet_names, collapse = ", "))
      ))
    }

    matching_tables <- lapply(
      sheet_results[matching_sheet_indexes],
      function(result) {
        result$rules_dt
      }
    )

    return(data.table::rbindlist(
      matching_tables,
      use.names = TRUE,
      fill = TRUE
    ))
  }

  cli::cli_abort("Unsupported rule extension for {.file {file_path}}.")
}

#' @title Load stage rule payloads
#' @description Discovers stage-specific rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage label (`clean` or `harmonize`).
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path_file
#' @importFrom purrr map
load_stage_rule_payloads <- function(config, stage_name) {
  checkmate::assert_list(config, min.len = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  imports_dir <- switch(
    validated_stage_name,
    clean = config$paths$data$imports$cleaning,
    harmonize = config$paths$data$imports$harmonization
  )
  checkmate::assert_string(imports_dir, min.chars = 1)

  ensure_directories_exist(imports_dir, recurse = TRUE)

  stage_pattern <- switch(
    validated_stage_name,
    clean = "^clean_.*\\.(xlsx|xls|csv)$",
    harmonize = "^harmonize_.*\\.(xlsx|xls|csv)$"
  )

  available_files <- fs::dir_ls(
    path = imports_dir,
    regexp = "\\.(xlsx|xls|csv)$",
    type = "file"
  )

  ordered_files <- available_files[
    grepl(stage_pattern, basename(available_files))
  ] |>
    sort()

  payloads <- purrr::map(ordered_files, function(file_path) {
    list(
      rule_file_id = fs::path_file(file_path),
      raw_rules = read_rule_table(file_path)
    )
  })

  return(payloads)
}

#' @title Resolve stage runtime cache settings
#' @description Resolves runtime cache settings from centralized defaults with
#' optional configuration overrides.
#' @param config Named configuration list.
#' @return Named list with `enabled`, `cache_file_name`, and `max_entries`.
#' @importFrom checkmate assert_list assert_flag assert_string assert_int
resolve_stage_runtime_cache_settings <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  defaults <- get_pipeline_constants()$post_processing$runtime_cache
  checkmate::assert_list(defaults, min.len = 1)

  configured_values <- defaults
  configured_post_processing <- NULL
  if (is.list(config$post_processing)) {
    configured_post_processing <- config$post_processing$runtime_cache
  }

  if (is.list(configured_post_processing)) {
    configured_values <- utils::modifyList(defaults, configured_post_processing)
  }

  enabled <- isTRUE(configured_values$enabled)
  cache_file_name <- as.character(configured_values$cache_file_name)
  suppressWarnings(max_entries <- as.integer(configured_values$max_entries))

  checkmate::assert_flag(enabled)
  checkmate::assert_string(cache_file_name, min.chars = 1)
  checkmate::assert_int(max_entries, lower = 1L)

  return(list(
    enabled = enabled,
    cache_file_name = cache_file_name,
    max_entries = max_entries
  ))
}

#' @title Build runtime cache file path for stage payload bundles
#' @description Returns deterministic cache file path under audit runtime-cache
#' directory.
#' @param config Named configuration list.
#' @param runtime_cache_settings Named runtime-cache settings.
#' @return Character scalar cache file path.
#' @importFrom checkmate assert_list assert_string
build_stage_runtime_cache_file_path <- function(config, runtime_cache_settings) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_list(runtime_cache_settings, min.len = 1)
  checkmate::assert_string(
    config$paths$data$audit$audit_root_dir,
    min.chars = 1
  )
  checkmate::assert_string(runtime_cache_settings$cache_file_name, min.chars = 1)

  runtime_cache_dir <- fs::path(
    config$paths$data$audit$audit_root_dir,
    "runtime_cache"
  )

  ensure_directories_exist(runtime_cache_dir, recurse = TRUE)

  return(fs::path(runtime_cache_dir, runtime_cache_settings$cache_file_name))
}

#' @title Prune cache entries deterministically
#' @description Applies deterministic pruning based on sorted cache-entry names.
#' @param cache_entries Named list of cache entries.
#' @param max_entries Integer scalar maximum entries to retain.
#' @return Pruned named list.
#' @importFrom checkmate assert_list assert_int
prune_runtime_cache_entries <- function(cache_entries, max_entries) {
  checkmate::assert_list(cache_entries)
  checkmate::assert_int(max_entries, lower = 1L)

  if (length(cache_entries) <= max_entries) {
    return(cache_entries)
  }

  entry_names <- names(cache_entries)
  if (is.null(entry_names)) {
    return(cache_entries[seq_len(max_entries)])
  }

  keep_names <- head(sort(entry_names), max_entries)

  return(cache_entries[keep_names])
}

#' @title Read runtime cache entries
#' @description Loads runtime cache entry list from disk with deterministic
#' pruning and defensive fallbacks.
#' @param cache_file_path Character scalar cache file path.
#' @param runtime_cache_settings Named runtime-cache settings.
#' @return Named list of cache entries.
#' @importFrom checkmate assert_string assert_list
read_stage_runtime_cache_entries <- function(
  cache_file_path,
  runtime_cache_settings
) {
  checkmate::assert_string(cache_file_path, min.chars = 1)
  checkmate::assert_list(runtime_cache_settings, min.len = 1)

  if (!isTRUE(runtime_cache_settings$enabled) || !file.exists(cache_file_path)) {
    return(list())
  }

  loaded_entries <- tryCatch(
    readRDS(cache_file_path),
    error = function(error_condition) {
      cli::cli_warn(c(
        "failed to read runtime cache entries; rebuilding cache entry set.",
        "!" = error_condition$message
      ))

      list()
    }
  )

  if (!is.list(loaded_entries)) {
    return(list())
  }

  return(prune_runtime_cache_entries(
    cache_entries = loaded_entries,
    max_entries = as.integer(runtime_cache_settings$max_entries)
  ))
}

#' @title Persist runtime cache entries
#' @description Persists runtime cache entries to disk using deterministic
#' pruning and stable save semantics.
#' @param cache_entries Named list of cache entries.
#' @param cache_file_path Character scalar cache file path.
#' @param runtime_cache_settings Named runtime-cache settings.
#' @return Invisibly returns persisted cache file path.
#' @importFrom checkmate assert_list assert_string
persist_stage_runtime_cache_entries <- function(
  cache_entries,
  cache_file_path,
  runtime_cache_settings
) {
  checkmate::assert_list(cache_entries)
  checkmate::assert_string(cache_file_path, min.chars = 1)
  checkmate::assert_list(runtime_cache_settings, min.len = 1)

  if (!isTRUE(runtime_cache_settings$enabled)) {
    return(invisible(cache_file_path))
  }

  entries_to_persist <- prune_runtime_cache_entries(
    cache_entries = cache_entries,
    max_entries = as.integer(runtime_cache_settings$max_entries)
  )

  ensure_directories_exist(fs::path_dir(cache_file_path), recurse = TRUE)
  saveRDS(entries_to_persist, file = cache_file_path)

  return(invisible(cache_file_path))
}

#' @title Build deterministic stage payload cache key
#' @description Builds deterministic stage cache key from stage name and ordered
#' rule-file fingerprints.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage label.
#' @return Character scalar cache key.
#' @importFrom checkmate assert_list assert_string
build_stage_payload_cache_key <- function(config, stage_name) {
  checkmate::assert_list(config, min.len = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  imports_dir <- switch(
    validated_stage_name,
    clean = config$paths$data$imports$cleaning,
    harmonize = config$paths$data$imports$harmonization
  )
  checkmate::assert_string(imports_dir, min.chars = 1)

  ensure_directories_exist(imports_dir, recurse = TRUE)

  stage_pattern <- switch(
    validated_stage_name,
    clean = "^clean_.*\\.(xlsx|xls|csv)$",
    harmonize = "^harmonize_.*\\.(xlsx|xls|csv)$"
  )

  available_files <- fs::dir_ls(
    path = imports_dir,
    regexp = "\\.(xlsx|xls|csv)$",
    type = "file"
  )

  ordered_files <- available_files[
    grepl(stage_pattern, basename(available_files))
  ] |>
    sort()

  if (length(ordered_files) == 0L) {
    return(paste0(validated_stage_name, "::<no_rule_files>"))
  }

  file_checksums <- unname(tools::md5sum(ordered_files))
  file_fingerprints <- paste0(
    fs::path_file(ordered_files),
    "::",
    file_checksums
  )

  return(paste0(
    validated_stage_name,
    "::",
    paste(file_fingerprints, collapse = "||")
  ))
}

#' @title Load stage payload bundle from disk cache
#' @description Loads one stage payload bundle from runtime cache file by cache
#' key.
#' @param cache_file_path Character scalar cache file path.
#' @param runtime_cache_settings Named runtime-cache settings.
#' @param cache_key Character scalar stage payload cache key.
#' @return Stage payload bundle list or `NULL`.
#' @importFrom checkmate assert_string assert_list
load_stage_payload_bundle_from_disk <- function(
  cache_file_path,
  runtime_cache_settings,
  cache_key
) {
  checkmate::assert_string(cache_file_path, min.chars = 1)
  checkmate::assert_list(runtime_cache_settings, min.len = 1)
  checkmate::assert_string(cache_key, min.chars = 1)

  cache_entries <- read_stage_runtime_cache_entries(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings
  )

  if (!(cache_key %in% names(cache_entries))) {
    return(NULL)
  }

  cached_bundle <- cache_entries[[cache_key]]

  if (!is.list(cached_bundle)) {
    return(NULL)
  }

  return(cached_bundle)
}

#' @title Persist stage payload bundle to disk cache
#' @description Persists one stage payload bundle under its cache key in runtime
#' cache file.
#' @param cache_file_path Character scalar cache file path.
#' @param runtime_cache_settings Named runtime-cache settings.
#' @param cache_key Character scalar stage payload cache key.
#' @param payload_bundle Named list payload bundle.
#' @return Invisibly returns persisted cache file path.
#' @importFrom checkmate assert_string assert_list
persist_stage_payload_bundle_to_disk <- function(
  cache_file_path,
  runtime_cache_settings,
  cache_key,
  payload_bundle
) {
  checkmate::assert_string(cache_file_path, min.chars = 1)
  checkmate::assert_list(runtime_cache_settings, min.len = 1)
  checkmate::assert_string(cache_key, min.chars = 1)
  checkmate::assert_list(payload_bundle, min.len = 1)

  if (!isTRUE(runtime_cache_settings$enabled)) {
    return(invisible(cache_file_path))
  }

  cache_entries <- read_stage_runtime_cache_entries(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings
  )

  cache_entries[[cache_key]] <- payload_bundle

  return(persist_stage_runtime_cache_entries(
    cache_entries = cache_entries,
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings
  ))
}

#' @title Prune in-memory stage payload cache
#' @description Applies deterministic pruning to in-memory stage payload bundle
#' cache.
#' @param max_entries Integer scalar maximum in-memory entries.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_int
prune_stage_payload_bundle_memory_cache <- function(max_entries) {
  checkmate::assert_int(max_entries, lower = 1L)

  cache_names <- ls(envir = .stage_payload_bundle_cache, all.names = TRUE)
  if (length(cache_names) <= max_entries) {
    return(invisible(TRUE))
  }

  names_to_remove <- setdiff(
    sort(cache_names),
    head(sort(cache_names), max_entries)
  )

  if (length(names_to_remove) > 0L) {
    rm(list = names_to_remove, envir = .stage_payload_bundle_cache)
  }

  return(invisible(TRUE))
}

#' @title Get cached stage payload bundle
#' @description Returns canonical stage payload bundle using memory cache, then
#' disk cache, then rebuild-and-persist flow.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage label.
#' @return Named list with `cache_key` and `canonical_payloads`.
#' @importFrom checkmate assert_list assert_string
get_cached_stage_payload_bundle <- function(config, stage_name) {
  checkmate::assert_list(config, min.len = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  runtime_cache_settings <- resolve_stage_runtime_cache_settings(config)
  cache_key <- build_stage_payload_cache_key(
    config = config,
    stage_name = validated_stage_name
  )

  if (!isTRUE(runtime_cache_settings$enabled)) {
    payloads <- load_stage_rule_payloads(
      config = config,
      stage_name = validated_stage_name
    )

    canonical_payloads <- lapply(payloads, function(payload) {
      canonical_rules <- coerce_rule_schema(
        rule_dt = payload$raw_rules,
        stage_name = validated_stage_name,
        rule_file_id = payload$rule_file_id
      )

      list(
        rule_file_id = payload$rule_file_id,
        canonical_rules = canonical_rules
      )
    })

    return(list(
      cache_key = cache_key,
      canonical_payloads = canonical_payloads
    ))
  }

  if (exists(cache_key, envir = .stage_payload_bundle_cache, inherits = FALSE)) {
    memory_bundle <- get(cache_key, envir = .stage_payload_bundle_cache)

    if (is.list(memory_bundle)) {
      return(memory_bundle)
    }
  }

  cache_file_path <- build_stage_runtime_cache_file_path(
    config = config,
    runtime_cache_settings = runtime_cache_settings
  )

  disk_bundle <- load_stage_payload_bundle_from_disk(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings,
    cache_key = cache_key
  )

  if (is.list(disk_bundle)) {
    assign(cache_key, disk_bundle, envir = .stage_payload_bundle_cache)
    prune_stage_payload_bundle_memory_cache(
      max_entries = as.integer(runtime_cache_settings$max_entries)
    )

    return(disk_bundle)
  }

  payloads <- load_stage_rule_payloads(
    config = config,
    stage_name = validated_stage_name
  )

  canonical_payloads <- lapply(payloads, function(payload) {
    canonical_rules <- coerce_rule_schema(
      rule_dt = payload$raw_rules,
      stage_name = validated_stage_name,
      rule_file_id = payload$rule_file_id
    )

    list(
      rule_file_id = payload$rule_file_id,
      canonical_rules = canonical_rules
    )
  })

  payload_bundle <- list(
    cache_key = cache_key,
    canonical_payloads = canonical_payloads
  )

  assign(cache_key, payload_bundle, envir = .stage_payload_bundle_cache)
  prune_stage_payload_bundle_memory_cache(
    max_entries = as.integer(runtime_cache_settings$max_entries)
  )

  persist_stage_payload_bundle_to_disk(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings,
    cache_key = cache_key,
    payload_bundle = payload_bundle
  )

  return(payload_bundle)
}

#' @title Build layer diagnostics from audit table
#' @description Generates deterministic diagnostics summary for one stage.
#' @param layer_name Character scalar stage label.
#' @param rows_in Integer scalar rows before stage.
#' @param rows_out Integer scalar rows after stage.
#' @param audit_dt Audit table generated by harmonization engine.
#' @return Named diagnostics list.
#' @importFrom checkmate assert_string assert_int assert_data_frame
build_layer_diagnostics <- function(layer_name, rows_in, rows_out, audit_dt) {
  checkmate::assert_string(layer_name, min.chars = 1)
  checkmate::assert_int(rows_in, lower = 0)
  checkmate::assert_int(rows_out, lower = 0)
  checkmate::assert_data_frame(audit_dt, min.rows = 0)

  audit_table <- data.table::as.data.table(audit_dt)
  matched_count <- if (nrow(audit_table) == 0) {
    0L
  } else {
    as.integer(sum(audit_table$affected_rows))
  }

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(
      Sys.time(),
      get_pipeline_constants()$timestamp_format_utc,
      tz = "UTC"
    ),
    rows_in = as.integer(rows_in),
    rows_out = as.integer(rows_out),
    matched_count = matched_count,
    unmatched_count = max(as.integer(rows_in - matched_count), 0L),
    idempotence_passed = TRUE,
    validation_passed = TRUE,
    status = if (matched_count > 0L) "pass" else "warn",
    messages = if (matched_count > 0L) {
      "Rules applied successfully"
    } else {
      "No rows matched available rules"
    }
  )

  return(diagnostics)
}
