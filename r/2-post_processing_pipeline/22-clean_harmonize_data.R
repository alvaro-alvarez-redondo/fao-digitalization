# script: clean and harmonize stage functions
# description: load stage-specific rule files and execute vectorized
# conditional transformations via a shared post-processing engine while
# preserving independent stage entry points.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(
    here::here("r", "0-general_pipeline", "01-setup.R"),
    echo = FALSE
  )
}

# In-memory cache of schema-validation dependency signatures.
.schema_validation_signature_cache <- new.env(parent = emptyenv())

#' @title Load cleaning rule payloads
#' @description Discovers cleaning rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list
load_cleaning_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  return(load_stage_rule_payloads(config = config, stage_name = "clean"))
}

#' @title Load harmonize rule payloads
#' @description Discovers harmonize rule files and returns deterministic payloads.
#' @param config Named configuration list.
#' @return List of payloads with `rule_file_id` and `raw_rules`.
#' @importFrom checkmate assert_list
load_harmonize_rule_payloads <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  return(load_stage_rule_payloads(config = config, stage_name = "harmonize"))
}

#' @title Canonicalize semicolon-delimited cells
#' @description Deduplicates and alphabetically sorts semicolon-delimited tokens
#' within each non-missing cell, then reconstructs deterministic cell strings.
#' @param values Atomic vector of cell values.
#' @param delimiter Character scalar output delimiter.
#' @return Character vector with canonicalized values.
canonicalize_semicolon_delimited_cells <- function(
  values,
  delimiter = get_pipeline_constants()$post_processing$target_update_strategies$concatenate_delimiter
) {
  checkmate::assert_atomic(values, any.missing = TRUE)
  checkmate::assert_string(delimiter, min.chars = 1)

  values_chr <- as.character(values)
  values_chr[is.na(values_chr) | trimws(values_chr) == ""] <- NA_character_

  non_missing_idx <- which(!is.na(values_chr))
  if (length(non_missing_idx) == 0L) {
    return(values_chr)
  }

  values_chr[non_missing_idx] <- vapply(
    values_chr[non_missing_idx],
    FUN.VALUE = character(1),
    FUN = function(single_value) {
      split_tokens <- strsplit(single_value, ";", fixed = TRUE)[[1]]
      split_tokens <- trimws(split_tokens)
      split_tokens <- split_tokens[nzchar(split_tokens)]

      if (length(split_tokens) == 0L) {
        return(NA_character_)
      }

      unique_tokens <- split_tokens[!duplicated(split_tokens)]
      sorted_tokens <- sort(unique_tokens, method = "radix")
      paste(sorted_tokens, collapse = delimiter)
    }
  )

  return(values_chr)
}

#' @title Canonicalize post-loop concatenated annotation columns
#' @description Applies per-cell semicolon token canonicalization to notes and
#' footnotes after stage loops complete, preserving global loop performance.
#' @param dataset_dt Data table mutated by reference.
#' @return Invisible logical scalar indicating whether any column was touched.
canonicalize_post_loop_annotation_columns <- function(dataset_dt) {
  checkmate::assert_data_table(dataset_dt)

  annotation_columns <- intersect(c("notes", "footnotes"), names(dataset_dt))
  if (length(annotation_columns) == 0L) {
    return(invisible(FALSE))
  }

  delimiter <- get_pipeline_constants()$post_processing$target_update_strategies$concatenate_delimiter

  for (column_name in annotation_columns) {
    data.table::set(
      dataset_dt,
      j = column_name,
      value = canonicalize_semicolon_delimited_cells(
        values = dataset_dt[[column_name]],
        delimiter = delimiter
      )
    )
  }

  return(invisible(TRUE))
}

#' @title Resolve stage multi-pass controls
#' @description Resolves and validates stage-specific multi-pass controls,
#' applying configuration overrides over centralized defaults.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage name.
#' @return Named list with enabled flag, max passes, cycle policy, and
#' diagnostics verbosity.
#' @importFrom checkmate assert_list assert_string assert_logical assert_integer
#'  assert_character
resolve_stage_multi_pass_controls <- function(config, stage_name) {
  checkmate::assert_list(config, min.len = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  defaults <- get_pipeline_constants()$post_processing$multi_pass
  checkmate::assert_list(defaults, min.len = 1)

  default_enabled_by_stage <- defaults$enabled_by_stage
  if (is.list(default_enabled_by_stage)) {
    default_enabled_by_stage <- unlist(
      default_enabled_by_stage,
      recursive = FALSE,
      use.names = TRUE
    )
  }
  checkmate::assert_logical(default_enabled_by_stage, any.missing = FALSE)

  default_max_passes_by_stage <- defaults$max_passes_by_stage
  if (is.list(default_max_passes_by_stage)) {
    default_max_passes_by_stage <- unlist(
      default_max_passes_by_stage,
      recursive = FALSE,
      use.names = TRUE
    )
  }
  if (!is.integer(default_max_passes_by_stage)) {
    suppressWarnings(
      default_max_passes_by_stage <- as.integer(
        default_max_passes_by_stage
      )
    )
  }
  checkmate::assert_integer(
    default_max_passes_by_stage,
    lower = 1L,
    any.missing = FALSE
  )

  configured_values <- defaults
  configured_multi_pass <- NULL
  if (is.list(config$post_processing)) {
    configured_multi_pass <- config$post_processing$multi_pass
  }
  if (is.list(configured_multi_pass)) {
    configured_values <- utils::modifyList(defaults, configured_multi_pass)
  }

  enabled_by_stage <- configured_values$enabled_by_stage
  if (is.list(enabled_by_stage)) {
    enabled_by_stage <- unlist(
      enabled_by_stage,
      recursive = FALSE,
      use.names = TRUE
    )
  }
  checkmate::assert_logical(enabled_by_stage, any.missing = FALSE)

  if (
    is.null(names(enabled_by_stage)) ||
      any(!nzchar(trimws(names(enabled_by_stage))))
  ) {
    cli::cli_abort("multi-pass enabled_by_stage must be a named logical vector")
  }

  if (!(validated_stage_name %in% names(enabled_by_stage))) {
    missing_enable_stages <- setdiff(
      names(default_enabled_by_stage),
      names(enabled_by_stage)
    )

    if (length(missing_enable_stages) > 0L) {
      enabled_by_stage <- c(
        enabled_by_stage,
        default_enabled_by_stage[missing_enable_stages]
      )
    }
  }

  enabled_by_stage <- enabled_by_stage[names(default_enabled_by_stage)]

  if (!(validated_stage_name %in% names(enabled_by_stage))) {
    cli::cli_abort(c(
      "multi-pass configuration is missing a stage enable flag.",
      "x" = paste0("stage: ", validated_stage_name)
    ))
  }

  max_passes_by_stage <- configured_values$max_passes_by_stage
  if (is.list(max_passes_by_stage)) {
    max_passes_by_stage <- unlist(
      max_passes_by_stage,
      recursive = FALSE,
      use.names = TRUE
    )
  }

  if (!is.integer(max_passes_by_stage)) {
    suppressWarnings(max_passes_by_stage <- as.integer(max_passes_by_stage))
  }
  checkmate::assert_integer(
    max_passes_by_stage,
    lower = 1L,
    any.missing = FALSE
  )

  if (
    is.null(names(max_passes_by_stage)) ||
      any(!nzchar(trimws(names(max_passes_by_stage))))
  ) {
    cli::cli_abort(
      "multi-pass max_passes_by_stage must be a named integer vector"
    )
  }

  if (!(validated_stage_name %in% names(max_passes_by_stage))) {
    missing_max_passes_stages <- setdiff(
      names(default_max_passes_by_stage),
      names(max_passes_by_stage)
    )

    if (length(missing_max_passes_stages) > 0L) {
      max_passes_by_stage <- c(
        max_passes_by_stage,
        default_max_passes_by_stage[missing_max_passes_stages]
      )
    }
  }

  max_passes_by_stage <- max_passes_by_stage[names(default_max_passes_by_stage)]

  if (!(validated_stage_name %in% names(max_passes_by_stage))) {
    cli::cli_abort(c(
      "multi-pass configuration is missing a stage max-pass value.",
      "x" = paste0("stage: ", validated_stage_name)
    ))
  }

  supported_cycle_policies <- configured_values$supported_cycle_policies
  checkmate::assert_character(
    supported_cycle_policies,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE
  )

  supported_diagnostics_verbosity <- configured_values$supported_diagnostics_verbosity
  checkmate::assert_character(
    supported_diagnostics_verbosity,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE
  )

  cycle_policy <- as.character(configured_values$cycle_policy)
  diagnostics_verbosity <- as.character(configured_values$diagnostics_verbosity)

  if (!(cycle_policy %in% supported_cycle_policies)) {
    cli::cli_abort(c(
      "invalid multi-pass cycle policy.",
      "x" = paste0("configured value: ", cycle_policy),
      "x" = paste0(
        "supported values: ",
        paste(supported_cycle_policies, collapse = ", ")
      )
    ))
  }

  if (!(diagnostics_verbosity %in% supported_diagnostics_verbosity)) {
    cli::cli_abort(c(
      "invalid multi-pass diagnostics verbosity.",
      "x" = paste0("configured value: ", diagnostics_verbosity),
      "x" = paste0(
        "supported values: ",
        paste(supported_diagnostics_verbosity, collapse = ", ")
      )
    ))
  }

  return(list(
    enabled = isTRUE(enabled_by_stage[[validated_stage_name]]),
    max_passes = as.integer(max_passes_by_stage[[validated_stage_name]]),
    cycle_policy = cycle_policy,
    diagnostics_verbosity = diagnostics_verbosity
  ))
}

#' @title Resolve schema-validation cache settings
#' @description Resolves schema-validation memoization settings from centralized
#' defaults with optional configuration overrides.
#' @param config Named configuration list.
#' @return Named list with `enabled` and `max_entries`.
#' @importFrom checkmate assert_list assert_flag assert_int
resolve_schema_validation_cache_settings <- function(config) {
  checkmate::assert_list(config, min.len = 1)

  defaults <- get_pipeline_constants()$post_processing$schema_validation_cache
  checkmate::assert_list(defaults, min.len = 1)

  configured_values <- defaults
  configured_post_processing <- NULL
  if (is.list(config$post_processing)) {
    configured_post_processing <- config$post_processing$schema_validation_cache
  }

  if (is.list(configured_post_processing)) {
    configured_values <- utils::modifyList(defaults, configured_post_processing)
  }

  enabled <- isTRUE(configured_values$enabled)
  suppressWarnings(max_entries <- as.integer(configured_values$max_entries))

  checkmate::assert_flag(enabled)
  checkmate::assert_int(max_entries, lower = 1L)

  return(list(
    enabled = enabled,
    max_entries = max_entries
  ))
}

#' @title Build schema-validation dependency signature
#' @description Builds deterministic dependency signature for schema validation
#' memoization.
#' @param dataset_dt Data table to validate against.
#' @param canonical_rules Canonical rule table.
#' @param stage_name Character scalar stage label.
#' @param rule_file_id Character scalar rule-file identifier.
#' @param payload_cache_key Character scalar stage payload cache key.
#' @return Character scalar dependency signature.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
build_schema_validation_dependency_signature <- function(
  dataset_dt,
  canonical_rules,
  stage_name,
  rule_file_id,
  payload_cache_key
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(canonical_rules, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(payload_cache_key, min.chars = 1)

  referenced_columns <- unique(c(
    canonical_rules$column_source,
    canonical_rules$column_target
  ))
  referenced_columns <- as.character(referenced_columns)
  referenced_columns <- trimws(referenced_columns)
  referenced_columns <- referenced_columns[
    !is.na(referenced_columns) & nzchar(referenced_columns)
  ]
  referenced_columns <- sort(unique(referenced_columns))

  referenced_column_classes <- vapply(
    referenced_columns,
    function(column_name) {
      if (!(column_name %in% names(dataset_dt))) {
        return("<missing>")
      }

      paste(class(dataset_dt[[column_name]]), collapse = "|")
    },
    character(1)
  )

  signature_payload <- list(
    stage_name = validated_stage_name,
    rule_file_id = rule_file_id,
    payload_cache_key = payload_cache_key,
    referenced_columns = referenced_columns,
    referenced_column_classes = referenced_column_classes
  )

  signature_raw <- serialize(
    signature_payload,
    connection = NULL,
    ascii = FALSE,
    version = 2
  )

  return(paste(as.integer(signature_raw), collapse = "-"))
}

#' @title Query schema-validation signature cache
#' @description Returns whether dependency signature is already memoized.
#' @param dependency_signature Character scalar dependency signature.
#' @param cache_settings Named schema-validation cache settings.
#' @return Logical scalar cache-hit flag.
#' @importFrom checkmate assert_string assert_list
is_schema_validation_signature_cached <- function(
  dependency_signature,
  cache_settings
) {
  checkmate::assert_string(dependency_signature, min.chars = 1)
  checkmate::assert_list(cache_settings, min.len = 1)

  if (!isTRUE(cache_settings$enabled)) {
    return(FALSE)
  }

  return(exists(
    dependency_signature,
    envir = .schema_validation_signature_cache,
    inherits = FALSE
  ))
}

#' @title Memoize schema-validation dependency signature
#' @description Stores dependency signature in memoization cache with
#' deterministic pruning.
#' @param dependency_signature Character scalar dependency signature.
#' @param cache_settings Named schema-validation cache settings.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_string assert_list
memoize_schema_validation_signature <- function(
  dependency_signature,
  cache_settings
) {
  checkmate::assert_string(dependency_signature, min.chars = 1)
  checkmate::assert_list(cache_settings, min.len = 1)

  if (!isTRUE(cache_settings$enabled)) {
    return(invisible(TRUE))
  }

  assign(
    dependency_signature,
    TRUE,
    envir = .schema_validation_signature_cache
  )

  cache_names <- ls(envir = .schema_validation_signature_cache, all.names = TRUE)
  max_entries <- as.integer(cache_settings$max_entries)

  if (length(cache_names) > max_entries) {
    names_to_remove <- setdiff(
      sort(cache_names),
      head(sort(cache_names), max_entries)
    )

    if (length(names_to_remove) > 0L) {
      rm(list = names_to_remove, envir = .schema_validation_signature_cache)
    }
  }

  return(invisible(TRUE))
}

#' @title Serialize stage state signature
#' @description Generates a deterministic raw signature for a stage state.
#' @param dataset_dt Stage dataset as data.table.
#' @return Raw vector state signature.
#' @importFrom checkmate assert_data_table
serialize_stage_state_signature <- function(dataset_dt) {
  checkmate::assert_data_table(dataset_dt)

  return(serialize(dataset_dt, connection = NULL, ascii = FALSE, version = 2))
}

#' @title Find repeated stage-state signature
#' @description Returns prior pass index when a state signature already exists.
#' @param state_signatures List of prior raw state signatures.
#' @param state_pass_indexes Integer vector pass indexes aligned to signatures.
#' @param candidate_signature Raw vector candidate signature.
#' @return Integer scalar repeated pass index or `NA_integer_`.
#' @importFrom checkmate assert_list assert_integer assert_raw
find_repeated_stage_state_pass <- function(
  state_signatures,
  state_pass_indexes,
  candidate_signature
) {
  checkmate::assert_list(state_signatures)
  checkmate::assert_integer(state_pass_indexes, any.missing = FALSE)
  checkmate::assert_raw(candidate_signature, min.len = 1)

  if (length(state_signatures) != length(state_pass_indexes)) {
    cli::cli_abort(
      "state-signature and pass-index vectors must have equal length"
    )
  }

  if (length(state_signatures) == 0L) {
    return(NA_integer_)
  }

  matches <- which(vapply(
    state_signatures,
    function(existing_signature) {
      identical(existing_signature, candidate_signature)
    },
    logical(1)
  ))

  if (length(matches) == 0L) {
    return(NA_integer_)
  }

  return(as.integer(state_pass_indexes[[matches[[1]]]]))
}

#' @title Run one rule-based post-processing stage
#' @description Applies one stage of rule payloads (`clean` or `harmonize`) and
#' returns transformed data with deterministic diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param stage_name Character scalar stage name (`clean` or `harmonize`).
#' @param dataset_name Character scalar dataset identifier.
#' @return `data.table` with attributes `layer_diagnostics` and `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom data.table as.data.table copy rbindlist
#' @importFrom purrr reduce
run_rule_stage_layer_batch <- function(
  dataset_dt,
  config,
  stage_name,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(stage_name, min.chars = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  payload_bundle <- get_cached_stage_payload_bundle(
    config = config,
    stage_name = validated_stage_name
  )

  canonical_payloads <- payload_bundle$canonical_payloads
  if (!is.list(canonical_payloads)) {
    canonical_payloads <- list()
  }

  rule_match_normalization_settings <- resolve_rule_match_normalization_settings()

  payload_cache_key <- payload_bundle$cache_key
  if (is.null(payload_cache_key)) {
    payload_cache_key <- paste0(validated_stage_name, "::<no_payload_cache_key>")
  }

  working_data <- data.table::copy(data.table::as.data.table(dataset_dt))

  schema_validation_cache_settings <- resolve_schema_validation_cache_settings(
    config = config
  )

  if (length(canonical_payloads) > 0L) {
    for (payload in canonical_payloads) {
      working_data <- ensure_rule_referenced_columns(
        dataset_dt = working_data,
        rules_dt = payload$canonical_rules
      )

      dependency_signature <- build_schema_validation_dependency_signature(
        dataset_dt = working_data,
        canonical_rules = payload$canonical_rules,
        stage_name = validated_stage_name,
        rule_file_id = payload$rule_file_id,
        payload_cache_key = payload_cache_key
      )

      if (!is_schema_validation_signature_cached(
        dependency_signature = dependency_signature,
        cache_settings = schema_validation_cache_settings
      )) {
        validate_canonical_rules(
          rules_dt = payload$canonical_rules,
          dataset_dt = working_data,
          rule_file_id = payload$rule_file_id,
          stage_name = validated_stage_name
        )

        memoize_schema_validation_signature(
          dependency_signature = dependency_signature,
          cache_settings = schema_validation_cache_settings
        )
      }
    }
  }

  multi_pass_controls <- resolve_stage_multi_pass_controls(
    config = config,
    stage_name = validated_stage_name
  )

  max_stage_passes <- if (isTRUE(multi_pass_controls$enabled)) {
    as.integer(multi_pass_controls$max_passes)
  } else {
    1L
  }

  execution_timestamp_utc <- format(
    Sys.time(),
    get_pipeline_constants()$timestamp_format_utc,
    tz = "UTC"
  )

  all_pass_audit_tables <- list()
  all_pass_overwrite_tables <- list()
  per_pass_diagnostics <- list()

  converged <- FALSE
  cycle_detected <- FALSE
  max_passes_reached_before_convergence <- FALSE
  cycle_message <- NULL
  max_passes_message <- NULL
  multi_pass_enabled <- isTRUE(multi_pass_controls$enabled)
  stage_stop_reason <- if (isTRUE(multi_pass_enabled)) {
    "converged_zero_change"
  } else {
    "single_pass_completed"
  }

  state_signatures <- list()
  state_pass_indexes <- integer(0)

  if (isTRUE(multi_pass_enabled)) {
    state_signatures <- list(serialize_stage_state_signature(working_data))
    state_pass_indexes <- c(0L)
  }

  for (pass_index in seq_len(max_stage_passes)) {
    pass_state <- list(
      data = working_data,
      audit_tables = list(),
      overwrite_tables = list(),
      changed_value_count = 0L
    )

    apply_match_normalization_for_pass <-
      isTRUE(rule_match_normalization_settings$apply_each_pass) ||
      (
        isTRUE(rule_match_normalization_settings$apply_once_before_stage) &&
          pass_index == 1L
      )

    if (length(canonical_payloads) > 0L) {
      pass_state <- purrr::reduce(
        .x = canonical_payloads,
        .init = pass_state,
        .f = function(state, payload) {
          if (nrow(payload$canonical_rules) == 0L) {
            return(state)
          }

          payload_result <- apply_rule_payload(
            dataset_dt = state$data,
            canonical_rules = payload$canonical_rules,
            stage_name = validated_stage_name,
            dataset_name = dataset_name,
            rule_file_id = payload$rule_file_id,
            execution_timestamp_utc = execution_timestamp_utc,
            apply_match_normalization = apply_match_normalization_for_pass
          )

          state$data <- payload_result$data
          state$audit_tables[[
            length(state$audit_tables) + 1L
          ]] <- payload_result$audit
          if (nrow(payload_result$overwrite_events) > 0L) {
            state$overwrite_tables[[
              length(state$overwrite_tables) + 1L
            ]] <- payload_result$overwrite_events
          }

          state$changed_value_count <-
            state$changed_value_count + payload_result$changed_value_count

          return(state)
        }
      )
    }

    pass_audit <- data.table::rbindlist(
      pass_state$audit_tables,
      use.names = TRUE,
      fill = TRUE
    )
    pass_audit[, loop := as.integer(pass_index)]

    pass_overwrite_events <- if (length(pass_state$overwrite_tables) > 0L) {
      data.table::rbindlist(
        pass_state$overwrite_tables,
        use.names = TRUE,
        fill = TRUE
      )
    } else {
      empty_last_rule_wins_overwrite_events_dt()
    }

    pass_matched_count <- if (nrow(pass_audit) == 0L) {
      0L
    } else {
      as.integer(sum(pass_audit$affected_rows))
    }

    pass_stop_reason <- "continued"
    current_signature <- NULL
    repeated_state_pass <- NA_integer_

    if (isTRUE(multi_pass_enabled)) {
      if (pass_state$changed_value_count == 0L) {
        # Zero changed values means pass output equals pass input.
        repeated_state_pass <- as.integer(pass_index - 1L)
        converged <- TRUE
        pass_stop_reason <- "converged_zero_change"
        stage_stop_reason <- pass_stop_reason
      } else {
        current_signature <- serialize_stage_state_signature(pass_state$data)
        repeated_state_pass <- find_repeated_stage_state_pass(
          state_signatures = state_signatures,
          state_pass_indexes = state_pass_indexes,
          candidate_signature = current_signature
        )

        if (!is.na(repeated_state_pass)) {
          if (repeated_state_pass == (pass_index - 1L)) {
            converged <- TRUE
            pass_stop_reason <- "converged_zero_change"
            stage_stop_reason <- pass_stop_reason
          } else {
            cycle_detected <- TRUE
            pass_stop_reason <- "cycle_detected"
            stage_stop_reason <- pass_stop_reason
            cycle_message <- paste0(
              "[",
              validated_stage_name,
              " stage] cycle detected at pass ",
              pass_index,
              " (repeats pass ",
              repeated_state_pass,
              ")."
            )

            if (identical(multi_pass_controls$cycle_policy, "abort")) {
              cli::cli_abort(c(
                "Post-processing multi-pass cycle detected.",
                "x" = cycle_message
              ))
            }

            cli::cli_warn(c(
              "Post-processing multi-pass cycle detected; stopping stage execution.",
              "!" = cycle_message
            ))
          }
        } else {
          state_signatures[[length(state_signatures) + 1L]] <- current_signature
          state_pass_indexes <- c(state_pass_indexes, as.integer(pass_index))
        }
      }
    }

    is_final_allowed_pass <- pass_index >= max_stage_passes
    should_warn_on_max_pass <-
      isTRUE(multi_pass_enabled) &&
      pass_stop_reason == "continued" &&
      is_final_allowed_pass

    should_stop_single_pass <-
      !isTRUE(multi_pass_enabled) &&
      pass_stop_reason == "continued" &&
      is_final_allowed_pass

    if (should_warn_on_max_pass) {
      max_passes_reached_before_convergence <- TRUE
      pass_stop_reason <- "max_passes_reached"
      stage_stop_reason <- pass_stop_reason
      max_passes_message <- paste0(
        "[",
        validated_stage_name,
        " stage] reached max_passes=",
        max_stage_passes,
        " before convergence."
      )

      cli::cli_warn(c(
        "Post-processing multi-pass max-pass limit reached.",
        "!" = max_passes_message
      ))
    }

    if (should_stop_single_pass) {
      pass_stop_reason <- "single_pass_completed"
      stage_stop_reason <- pass_stop_reason
    }

    per_pass_diagnostics[[length(per_pass_diagnostics) + 1L]] <-
      data.table::data.table(
        pass_index = as.integer(pass_index),
        changed_value_count = as.integer(pass_state$changed_value_count),
        matched_count = as.integer(pass_matched_count),
        audit_rows = as.integer(nrow(pass_audit)),
        overwrite_event_rows = as.integer(nrow(pass_overwrite_events)),
        repeated_state_pass = repeated_state_pass,
        stop_reason = pass_stop_reason
      )

    all_pass_audit_tables[[length(all_pass_audit_tables) + 1L]] <- pass_audit

    if (nrow(pass_overwrite_events) > 0L) {
      all_pass_overwrite_tables[[
        length(all_pass_overwrite_tables) + 1L
      ]] <- pass_overwrite_events
    }

    working_data <- pass_state$data

    if (!identical(pass_stop_reason, "continued")) {
      break
    }
  }

  canonicalize_post_loop_annotation_columns(working_data)

  stage_audit <- data.table::rbindlist(
    all_pass_audit_tables,
    use.names = TRUE,
    fill = TRUE
  )

  stage_overwrite_events <- if (length(all_pass_overwrite_tables) > 0L) {
    data.table::rbindlist(
      all_pass_overwrite_tables,
      use.names = TRUE,
      fill = TRUE
    )
  } else {
    empty_last_rule_wins_overwrite_events_dt()
  }

  pass_diagnostics_dt <- data.table::rbindlist(
    per_pass_diagnostics,
    use.names = TRUE,
    fill = TRUE
  )

  diagnostics <- build_layer_diagnostics(
    layer_name = validated_stage_name,
    rows_in = nrow(dataset_dt),
    rows_out = nrow(working_data),
    audit_dt = stage_audit
  )

  diagnostics_messages <- diagnostics$messages
  if (is.null(diagnostics_messages)) {
    diagnostics_messages <- character(0)
  }
  diagnostics_messages <- as.character(diagnostics_messages)
  diagnostics_messages <- diagnostics_messages[!is.na(diagnostics_messages)]

  multi_pass_summary <- paste0(
    "[",
    validated_stage_name,
    " stage] multi-pass stop_reason=",
    stage_stop_reason,
    "; passes_executed=",
    nrow(pass_diagnostics_dt),
    "; max_passes=",
    max_stage_passes,
    "; enabled=",
    tolower(as.character(multi_pass_enabled)),
    "."
  )

  diagnostics_messages <- c(diagnostics_messages, multi_pass_summary)
  if (!is.null(cycle_message)) {
    diagnostics_messages <- c(diagnostics_messages, cycle_message)
  }
  if (!is.null(max_passes_message)) {
    diagnostics_messages <- c(diagnostics_messages, max_passes_message)
  }

  diagnostics$messages <- diagnostics_messages
  diagnostics$multi_pass <- list(
    enabled = isTRUE(multi_pass_enabled),
    max_passes = as.integer(max_stage_passes),
    passes_executed = as.integer(nrow(pass_diagnostics_dt)),
    converged = isTRUE(converged),
    cycle_detected = isTRUE(cycle_detected),
    max_passes_reached_before_convergence = isTRUE(
      max_passes_reached_before_convergence
    ),
    cycle_policy = multi_pass_controls$cycle_policy,
    diagnostics_verbosity = multi_pass_controls$diagnostics_verbosity,
    stop_reason = stage_stop_reason
  )

  if (identical(multi_pass_controls$diagnostics_verbosity, "verbose")) {
    diagnostics$multi_pass$pass_diagnostics <- pass_diagnostics_dt
  }

  stage_output_dt <- working_data
  attr(stage_output_dt, "layer_diagnostics") <- diagnostics
  attr(stage_output_dt, "layer_audit") <- stage_audit
  attr(stage_output_dt, "layer_last_rule_wins_overwrites") <-
    stage_overwrite_events
  attr(stage_output_dt, "layer_multi_pass_diagnostics") <- pass_diagnostics_dt

  return(stage_output_dt)
}

#' @title Run cleaning layer batch
#' @description Applies clean-stage conditional rules and returns cleaned data
#' with diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Cleaned `data.table` with attributes `layer_diagnostics` and
#' `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_cleaning_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_rule_stage_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "clean",
    dataset_name = dataset_name
  ))
}

#' @title Run harmonize layer batch
#' @description Applies harmonize-stage conditional rules and returns harmonized
#' data with diagnostics and audit metadata.
#' @param dataset_dt Input dataset as data.frame/data.table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Harmonized `data.table` with attributes `layer_diagnostics` and
#' `layer_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_harmonize_layer_batch <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_rule_stage_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    stage_name = "harmonize",
    dataset_name = dataset_name
  ))
}
