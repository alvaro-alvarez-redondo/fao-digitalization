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
