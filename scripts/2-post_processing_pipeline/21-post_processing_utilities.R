# script: post-processing utilities
# description: reusable stage metadata, template generation, rule validation,
# dictionary construction, vectorized harmonization engine, and structured
# audit helpers for post-processing stages.

#' @title Get canonical rule columns
#' @description Returns stage-specific canonical rule column names.
#' @param stage_name Optional character scalar stage label (`clean` or
#' `harmonize`). When `NULL`, defaults to clean columns.
#' @return Character vector of canonical columns.
#' @examples
#' get_canonical_rule_columns("clean")
get_canonical_rule_columns <- function(stage_name = NULL) {
  if (is.null(stage_name)) {
    stage_name <- "clean"
  }

  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  if (identical(validated_stage_name, "harmonize")) {
    return(c(
      "column_source",
      "value_source_raw",
      "value_source_harmonize",
      "column_target",
      "value_target_raw",
      "value_target_harmonize"
    ))
  }

  return(c(
    "column_source",
    "value_source_raw",
    "value_source_clean",
    "column_target",
    "value_target_raw",
    "value_target_clean"
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

#' @title Validate post-processing stage name
#' @description Ensures stage name is one of the supported post-processing stages.
#' @param stage_name Character scalar stage label.
#' @return Character scalar validated stage name.
#' @importFrom checkmate assert_string
validate_post_processing_stage_name <- function(stage_name) {
  checkmate::assert_string(stage_name, min.chars = 1)
  validated_stage_name <- match.arg(stage_name, choices = get_post_processing_stage_names())

  return(validated_stage_name)
}

#' @title Get canonical target value column for stage
#' @description Returns stage-specific target value column name.
#' @param stage_name Character scalar stage label.
#' @return Character scalar target value column name.
get_stage_target_value_column <- function(stage_name) {
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  if (identical(validated_stage_name, "harmonize")) {
    return("value_target_harmonize")
  }

  return("value_target_clean")
}

#' @title Get canonical source value column for stage
#' @description Returns stage-specific source value column name.
#' @param stage_name Character scalar stage label.
#' @return Character scalar source value column name.
get_stage_source_value_column <- function(stage_name) {
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  if (identical(validated_stage_name, "harmonize")) {
    return("value_source_harmonize")
  }

  return("value_source_clean")
}

#' @title Get stage-specific rule template columns
#' @description Returns canonical stage columns for template generation.
#' @param stage_name Character scalar stage label.
#' @return Character vector of template columns.
#' @importFrom checkmate assert_string
get_stage_rule_template_columns <- function(stage_name) {
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  return(get_canonical_rule_columns(validated_stage_name))
}

#' @title Get post-processing audit paths
#' @description Resolves deterministic audit root and subdirectory paths.
#' @param config Named configuration list.
#' @return Named list with `audit_root_dir`, `diagnostics_dir`, and `templates_dir`.
#' @importFrom checkmate assert_list assert_string
get_post_processing_audit_paths <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$audit$audit_root_dir, min.chars = 1)

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
  ensure_directories_exist(unlist(audit_paths, use.names = FALSE), recurse = TRUE)

  return(audit_paths)
}

#' @title Generate one stage rule template workbook
#' @description Writes a deterministic template workbook with stage-prefixed
#' rule columns and guidance under the audit template directory.
#' @param stage_name Character scalar stage label.
#' @param audit_paths Named list from `get_post_processing_audit_paths()`.
#' @param overwrite Logical scalar indicating whether existing template is replaced.
#' @return Character scalar written template path.
#' @importFrom checkmate assert_string assert_list assert_flag
write_stage_rule_template <- function(stage_name, audit_paths, overwrite = TRUE) {
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_list(audit_paths, min.len = 1)
  checkmate::assert_string(audit_paths$templates_dir, min.chars = 1)
  checkmate::assert_flag(overwrite)

  template_columns <- get_stage_rule_template_columns(validated_stage_name)
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
    paste0(validated_stage_name, "_rules_template.xlsx")
  )

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "rules_template")
  openxlsx::writeData(workbook, "rules_template", template_data)
  openxlsx::addWorksheet(workbook, "guidance")
  openxlsx::writeData(workbook, "guidance", guidance_data)
  openxlsx::saveWorkbook(workbook, template_path, overwrite = overwrite)

  return(template_path)
}

#' @title Generate post-processing rule templates
#' @description Writes clean and harmonize templates under `audit_root_dir/templates`.
#' @param config Named configuration list.
#' @param overwrite Logical scalar indicating whether existing templates are replaced.
#' @return Named character vector of generated template paths.
#' @importFrom checkmate assert_list assert_flag
generate_post_processing_rule_templates <- function(config, overwrite = TRUE) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_flag(overwrite)

  audit_paths <- initialize_post_processing_audit_root(config)
  stage_names <- get_post_processing_stage_names()

  template_paths <- vapply(
    stage_names,
    function(stage_name) {
      write_stage_rule_template(
        stage_name = stage_name,
        audit_paths = audit_paths,
        overwrite = overwrite
      )
    },
    character(1)
  )

  names(template_paths) <- stage_names

  return(template_paths)
}

#' @title Read rule table from csv or excel
#' @description Reads a rule table file and returns a `data.table`.
#' @param file_path Character scalar path to rule file.
#' @return `data.table` containing rule rows.
#' @importFrom checkmate assert_string assert_file_exists
#' @importFrom fs path_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @examples
#' \dontrun{read_rule_table("data/1-import/11-clean_imports/clean_rules.xlsx")}
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  file_extension <- fs::path_ext(file_path) |>
    tolower()

  if (identical(file_extension, "csv")) {
    return(readr::read_csv(file_path, show_col_types = FALSE) |> data.table::as.data.table())
  }

  if (file_extension %in% c("xlsx", "xls")) {
    return(readxl::read_excel(file_path) |> data.table::as.data.table())
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

#' @title Coerce rule schema to canonical columns
#' @description Enforces strict stage-specific canonical schema.
#' @param rule_dt Rule table as data.frame/data.table.
#' @param stage_name Character scalar execution stage label.
#' @param rule_file_id Character scalar rule file identifier.
#' @return Canonicalized `data.table` rule table.
#' @importFrom checkmate assert_data_frame assert_string
coerce_rule_schema <- function(rule_dt, stage_name, rule_file_id) {
  checkmate::assert_data_frame(rule_dt, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  canonical_columns <- get_canonical_rule_columns(validated_stage_name)
  available_columns <- colnames(rule_dt)

  source_result_column <- get_stage_source_value_column(validated_stage_name)
  optional_columns <- source_result_column

  missing_columns <- setdiff(canonical_columns, available_columns)
  missing_required_columns <- setdiff(missing_columns, optional_columns)

  if (length(missing_required_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} is missing required columns.",
      "x" = paste(missing_required_columns, collapse = ", ")
    ))
  }

  unexpected_columns <- setdiff(available_columns, canonical_columns)
  if (length(unexpected_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains unexpected columns.",
      "x" = paste(unexpected_columns, collapse = ", ")
    ))
  }

  canonical_dt <- data.table::as.data.table(rule_dt)

  if (!(source_result_column %in% colnames(canonical_dt))) {
    canonical_dt[, (source_result_column) := NA_character_]
  }

  canonical_dt <- canonical_dt[, ..canonical_columns]

  return(canonical_dt)
}

#' @title Normalize permitted missing rule values for internal validation
#' @description Converts allowed missing values in conditional rule value fields
#' to an internal placeholder used only during validation joins/grouping, while
#' preserving original rule semantics for downstream application.
#' @param rules_dt Canonical rule table.
#' @param stage_name Character scalar stage label.
#' @param na_placeholder Character scalar internal placeholder token.
#' @return Named list with `rules_for_validation` and `allowed_na_columns`.
#' @importFrom checkmate assert_data_frame assert_string
normalize_rule_values_for_validation <- function(
  rules_dt,
  stage_name,
  na_placeholder = "..NA_INTERNAL.."
) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  allowed_na_columns <- intersect(
    c(
      "value_source_raw",
      "value_source_clean",
      "value_source_harmonize",
      "value_target_raw",
      "value_target_clean",
      "value_target_harmonize"
    ),
    colnames(rules_dt)
  )

  rules_for_validation <- data.table::copy(data.table::as.data.table(rules_dt))

  if (length(allowed_na_columns) > 0L) {
    rules_for_validation[
      ,
      (allowed_na_columns) := lapply(.SD, function(column_values) {
        replacement_values <- column_values

        if (is.character(replacement_values)) {
          replacement_values[trimws(replacement_values) == ""] <- na_placeholder
          replacement_values[is.na(replacement_values)] <- na_placeholder
        }

        return(replacement_values)
      }),
      .SDcols = allowed_na_columns
    ]
  }

  return(list(
    rules_for_validation = rules_for_validation,
    allowed_na_columns = allowed_na_columns
  ))
}

#' @title Ensure rule-referenced dataset columns exist
#' @description Adds any missing `column_source` or `column_target` columns
#' referenced by canonical rules to the dataset as `NA_character_` before
#' validation and rule execution.
#' @param dataset_dt Data table to mutate by reference.
#' @param rules_dt Canonical rule table.
#' @return Mutated `dataset_dt` with missing referenced columns initialized.
#' @importFrom checkmate assert_data_table assert_data_frame
#' @importFrom data.table setcolorder
ensure_rule_referenced_columns <- function(dataset_dt, rules_dt) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(rules_dt, min.rows = 0)

  if (nrow(rules_dt) == 0L) {
    return(dataset_dt)
  }

  referenced_columns <- unique(c(rules_dt$column_source, rules_dt$column_target))
  referenced_columns <- referenced_columns[!is.na(referenced_columns) & nzchar(referenced_columns)]

  if (length(referenced_columns) == 0L) {
    return(dataset_dt)
  }

  existing_columns <- colnames(dataset_dt)
  missing_columns <- setdiff(referenced_columns, existing_columns)

  if (length(missing_columns) > 0L) {
    dataset_dt[, (missing_columns) := NA_character_]

    data.table::setcolorder(
      dataset_dt,
      c(existing_columns, missing_columns)
    )
  }

  return(dataset_dt)
}

#' @title Validate canonical rules
#' @description Validates schema completeness, dataset-column presence, rule-key
#' uniqueness, conflict-free mappings, and type compatibility.
#' @param rules_dt Canonical rule table.
#' @param dataset_dt Dataset to mutate.
#' @param rule_file_id Character scalar rule file identifier.
#' @param stage_name Character scalar execution stage label.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame assert_string
validate_canonical_rules <- function(rules_dt, dataset_dt, rule_file_id, stage_name) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  required_columns <- get_canonical_rule_columns(validated_stage_name)
  missing_rule_columns <- setdiff(required_columns, colnames(rules_dt))
  if (length(missing_rule_columns) > 0) {
    cli::cli_abort(c(
      "Canonical rule schema validation failed for {.file {rule_file_id}}.",
      "x" = paste(missing_rule_columns, collapse = ", ")
    ))
  }

  if (nrow(rules_dt) == 0) {
    return(invisible(TRUE))
  }

  validation_context <- normalize_rule_values_for_validation(
    rules_dt = rules_dt,
    stage_name = validated_stage_name
  )
  rules_for_validation <- validation_context$rules_for_validation
  allowed_na_columns <- validation_context$allowed_na_columns

  strict_required_columns <- setdiff(required_columns, allowed_na_columns)
  columns_with_na <- strict_required_columns[vapply(
    strict_required_columns,
    function(column_name) {
      anyNA(rules_dt[[column_name]])
    },
    logical(1)
  )]

  if (length(columns_with_na) > 0) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains missing values in required columns.",
      "x" = paste(columns_with_na, collapse = ", ")
    ))
  }

  dataset_columns <- colnames(dataset_dt)
  missing_source <- setdiff(unique(rules_dt$column_source), dataset_columns)
  missing_target <- setdiff(unique(rules_dt$column_target), dataset_columns)

  if (length(missing_source) > 0 || length(missing_target) > 0) {
    cli::cli_abort(c(
      "Rule columns are not present in dataset for {.file {rule_file_id}}.",
      if (length(missing_source) > 0) paste0("x source: ", paste(missing_source, collapse = ", ")),
      if (length(missing_target) > 0) paste0("x target: ", paste(missing_target, collapse = ", "))
    ))
  }

  duplicate_key_dt <- rules_for_validation[
    ,
    .N,
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][N > 1L]

  if (nrow(duplicate_key_dt) > 0) {
    cli::cli_abort(c(
      "Rule uniqueness validation failed for {.file {rule_file_id}}.",
      "x" = "Each (column_source, value_source_raw, column_target, value_target_raw) must be unique."
    ))
  }

  target_value_column <- get_stage_target_value_column(validated_stage_name)
  source_value_column <- get_stage_source_value_column(validated_stage_name)

  conflict_dt <- rules_for_validation[
    ,
    .(target_value_count = uniqueN(get(target_value_column))),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][target_value_count > 1L]

  if (nrow(conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting rules detected in {.file {rule_file_id}}.",
      "x" = "A single source/target key maps to multiple target values."
    ))
  }

  source_conflict_dt <- rules_for_validation[
    ,
    .(source_value_count = uniqueN(get(source_value_column))),
    by = .(column_source, value_source_raw)
  ][source_value_count > 1L]

  if (nrow(source_conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting source rewrite rules detected in {.file {rule_file_id}}.",
      "x" = "A single (column_source, value_source_raw) maps to multiple source result values."
    ))
  }

  check_type_compatibility <- function(column_name, rule_values, field_name) {
    dataset_vector <- dataset_dt[[column_name]]
    non_missing_values <- rule_values[!is.na(rule_values)]

    if (is.factor(dataset_vector)) {
      non_missing_values <- as.character(non_missing_values)
    }

    if (is.numeric(dataset_vector)) {
      suppressWarnings(parsed_values <- as.numeric(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to numeric for column ", column_name)
        ))
      }
    }

    if (is.integer(dataset_vector)) {
      suppressWarnings(parsed_values <- as.integer(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to integer for column ", column_name)
        ))
      }
    }

    if (inherits(dataset_vector, "Date")) {
      suppressWarnings(parsed_values <- as.Date(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to Date for column ", column_name)
        ))
      }
    }

    return(invisible(TRUE))
  }

  rules_dt[, check_type_compatibility(column_source[1], value_source_raw, "value_source_raw"), by = column_source]
  rules_dt[, check_type_compatibility(column_target[1], value_target_raw, "value_target_raw"), by = column_target]

  rules_with_source_result <- rules_dt[!is.na(get(source_value_column))]
  if (nrow(rules_with_source_result) > 0L) {
    rules_with_source_result[
      ,
      check_type_compatibility(
        column_source[1],
        get(source_value_column),
        source_value_column
      ),
      by = column_source
    ]
  }

  return(invisible(TRUE))
}

#' @title Build conditional dictionaries from canonical rules
#' @description Groups canonical rules by `(column_source, column_target)` and
#' sorts deterministically for reproducible execution.
#' @param rules_dt Canonical rules table.
#' @return List of grouped rule tables.
#' @importFrom checkmate assert_data_frame
build_conditional_rule_dictionary <- function(rules_dt, stage_name) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  if (nrow(rules_dt) == 0L) {
    return(list())
  }

  target_value_column <- get_stage_target_value_column(validated_stage_name)

  ordered_rules <- data.table::as.data.table(rules_dt)[order(
    column_source,
    column_target,
    value_source_raw,
    value_target_raw,
    get(target_value_column)
  )]

  grouped_rules <- split(
    x = ordered_rules,
    f = interaction(ordered_rules$column_source, ordered_rules$column_target, drop = TRUE),
    drop = TRUE
  )

  return(grouped_rules)
}

#' @title Encode target rule values with internal missing placeholder
#' @description Converts empty strings and missing values in target rule values
#' to an explicit internal placeholder for deterministic downstream handling.
#' @param values Atomic vector values to encode.
#' @param na_placeholder Character scalar internal missing token.
#' @return Character vector with placeholder-encoded missing values.
#' @importFrom checkmate assert_atomic assert_string
encode_target_rule_value <- function(values, na_placeholder = "..NA_INTERNAL..") {
  checkmate::assert_atomic(values, min.len = 0, any.missing = TRUE)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  if (length(values) == 0L) {
    return(character(0))
  }

  encoded_values <- as.character(values)
  encoded_values[trimws(encoded_values) == ""] <- na_placeholder
  encoded_values[is.na(encoded_values)] <- na_placeholder

  return(encoded_values)
}

#' @title Decode internal placeholder back to `NA_character_`
#' @description Reverts encoded missing target values to canonical
#' `NA_character_` representation before rule application.
#' @param values Character vector values to decode.
#' @param na_placeholder Character scalar internal missing token.
#' @return Character vector with placeholder decoded to `NA_character_`.
#' @importFrom checkmate assert_character assert_string
decode_target_rule_value <- function(values, na_placeholder = "..NA_INTERNAL..") {
  checkmate::assert_character(values, any.missing = TRUE)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  decoded_values <- values
  decoded_values[decoded_values == na_placeholder] <- NA_character_

  return(decoded_values)
}

#' @title Build deterministic matching keys with explicit NA handling
#' @description Normalizes values to comparable string keys and maps missing
#' values to an explicit internal token to guarantee deterministic NA matching
#' behavior during join operations.
#' @param values Atomic vector values to encode.
#' @param na_key Character scalar NA token used for matching keys.
#' @return Character vector key.
#' @importFrom checkmate assert_atomic assert_string
encode_rule_match_key <- function(values, na_key = "..NA_MATCH_KEY..") {
  checkmate::assert_atomic(values, min.len = 0, any.missing = TRUE)
  checkmate::assert_string(na_key, min.chars = 1)

  if (length(values) == 0L) {
    return(character(0))
  }

  encoded_key <- normalize_string(values)
  encoded_key[is.na(encoded_key)] <- na_key

  return(encoded_key)
}

#' @title Apply one conditional dictionary group
#' @description Executes vectorized matching and mutation for one
#' `(column_source, column_target)` group and captures structured audit records.
#' @param dataset_dt Data table to mutate.
#' @param group_rules Canonical rules for one source-target column pair.
#' @param stage_name Character scalar stage label.
#' @param dataset_name Character scalar dataset identifier.
#' @param rule_file_id Character scalar rule file identifier.
#' @param execution_timestamp_utc Character scalar execution timestamp.
#' @return List with mutated `data` and `audit` table.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
apply_conditional_rule_group <- function(
  dataset_dt,
  group_rules,
  stage_name,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(group_rules, min.rows = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  target_value_column <- get_stage_target_value_column(validated_stage_name)
  source_value_column <- get_stage_source_value_column(validated_stage_name)

  group_dt <- data.table::as.data.table(group_rules)
  source_column <- group_dt$column_source[[1]]
  target_column <- group_dt$column_target[[1]]

  normalized_rules <- unique(group_dt[, .(
    column_source,
    value_source_raw,
    source_value_raw = get(source_value_column),
    column_target,
    value_target_raw,
    value_target_result_placeholder = encode_target_rule_value(get(target_value_column)),
    source_key = encode_rule_match_key(value_source_raw),
    target_key = encode_rule_match_key(value_target_raw)
  )][
    ,
    `:=`(
      value_source_result = as.character(source_value_raw),
      value_target_result = decode_target_rule_value(value_target_result_placeholder)
    )
  ])

  normalized_rules[trimws(value_source_result) == "", value_source_result := NA_character_]

  source_values_pre_update <- dataset_dt[[source_column]]
  target_values_pre_update <- dataset_dt[[target_column]]

  join_input <- data.table::data.table(
    source_key = encode_rule_match_key(source_values_pre_update),
    target_key = encode_rule_match_key(target_values_pre_update)
  )

  joined_dt <- normalized_rules[
    join_input,
    on = .(source_key, target_key)
  ]

  matched_row_mask <- !is.na(joined_dt$column_source)
  source_update_mask <- matched_row_mask & !is.na(joined_dt$value_source_result)
  matched_rows <- as.integer(sum(matched_row_mask))

  if (matched_rows > 0L) {
    if (identical(source_column, target_column)) {
      if (any(source_update_mask)) {
        dataset_dt[
          source_update_mask,
          (source_column) := joined_dt$value_source_result[source_update_mask]
        ]
      }

      dataset_dt[
        matched_row_mask,
        (target_column) := joined_dt$value_target_result[matched_row_mask]
      ]
    } else {
      if (any(source_update_mask)) {
        dataset_dt[
          source_update_mask,
          (source_column) := joined_dt$value_source_result[source_update_mask]
        ]
      }

      dataset_dt[
        matched_row_mask,
        (target_column) := joined_dt$value_target_result[matched_row_mask]
      ]
    }
  }

  matched_counts <- joined_dt[matched_row_mask, .(
    affected_rows = .N
  ), by = .(
    source_key,
    target_key,
    value_source_result,
    value_target_result_placeholder
  )]

  audit_dt <- normalized_rules[
    matched_counts,
    on = .(
      source_key,
      target_key,
      value_source_result,
      value_target_result_placeholder
    )
  ][
    ,
    .(
      dataset_name = dataset_name,
      column_source,
      value_source_raw,
      value_source_result,
      column_target,
      value_target_raw,
      value_target_result,
      affected_rows = data.table::fcoalesce(affected_rows, 0L),
      execution_timestamp_utc = execution_timestamp_utc,
      rule_file_identifier = rule_file_id,
      execution_stage = validated_stage_name
    )
  ][order(column_source, column_target, value_source_raw, value_target_raw)]

  return(list(data = dataset_dt, audit = audit_dt))
}

#' @title Apply canonical rule file payload
#' @description Executes matching and mutation in deterministic group order for a
#' single file payload.
#' @param dataset_dt Data table to mutate.
#' @param canonical_rules Canonical rules table.
#' @param stage_name Character scalar stage label.
#' @param dataset_name Character scalar dataset identifier.
#' @param rule_file_id Character scalar rule file identifier.
#' @param execution_timestamp_utc Character scalar execution timestamp.
#' @return List with mutated `data` and aggregated `audit` table.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
apply_rule_payload <- function(
  dataset_dt,
  canonical_rules,
  stage_name,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(canonical_rules, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  grouped_dictionary <- build_conditional_rule_dictionary(canonical_rules, validated_stage_name)

  if (length(grouped_dictionary) == 0) {
    return(list(data = dataset_dt, audit = data.table::data.table()))
  }

  state <- purrr::reduce(
    .x = grouped_dictionary,
    .init = list(data = dataset_dt, audit = list()),
    .f = function(current_state, group_rules) {
      group_result <- apply_conditional_rule_group(
        dataset_dt = current_state$data,
        group_rules = group_rules,
        stage_name = validated_stage_name,
        dataset_name = dataset_name,
        rule_file_id = rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      current_state$data <- group_result$data
      current_state$audit[[length(current_state$audit) + 1L]] <- group_result$audit

      return(current_state)
    }
  )

  combined_audit <- data.table::rbindlist(state$audit, use.names = TRUE, fill = TRUE)

  return(list(data = state$data, audit = combined_audit))
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
  matched_count <- if (nrow(audit_table) == 0) 0L else as.integer(sum(audit_table$affected_rows))

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
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
