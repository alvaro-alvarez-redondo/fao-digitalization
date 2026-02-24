# script: cleaning and harmonization pipeline functions
# description: rule-driven cleaning and harmonization and orchestration helpers.

#' @title build standardized layer diagnostics object
#' @description create a one-row diagnostics object following the phase-1
#' contract.
#' @param layer_name character scalar layer name.
#' @param rule_version_cleaning character scalar or `NA_character_`.
#' @param rule_version_taxonomy character scalar or `NA_character_`.
#' @param rule_version_conversion character scalar or `NA_character_`.
#' @param rows_in integer scalar row count before layer.
#' @param rows_out integer scalar row count after layer.
#' @param matched_count integer scalar.
#' @param unmatched_count integer scalar.
#' @param idempotence_passed logical scalar.
#' @param validation_passed logical scalar.
#' @param status character scalar one of `pass`, `warn`, `fail`.
#' @param messages character vector of diagnostics messages.
#' @return named list diagnostics object.
#' @importFrom checkmate assert_string assert_int assert_flag assert_choice assert_character
#' @examples
#' build_layer_diagnostics("cleaning", rows_in = 10L, rows_out = 10L)
build_layer_diagnostics <- function(
  layer_name,
  rule_version_cleaning = NA_character_,
  rule_version_taxonomy = NA_character_,
  rule_version_conversion = NA_character_,
  rows_in,
  rows_out,
  matched_count = 0L,
  unmatched_count = 0L,
  idempotence_passed = TRUE,
  validation_passed = TRUE,
  status = "pass",
  messages = character(0)
) {
  checkmate::assert_string(layer_name, min.chars = 1)
  checkmate::assert_int(rows_in, lower = 0)
  checkmate::assert_int(rows_out, lower = 0)
  checkmate::assert_int(matched_count, lower = 0)
  checkmate::assert_int(unmatched_count, lower = 0)
  checkmate::assert_flag(idempotence_passed)
  checkmate::assert_flag(validation_passed)
  checkmate::assert_choice(status, c("pass", "warn", "fail"))
  checkmate::assert_character(messages, any.missing = FALSE)

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(
      Sys.time(),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    ),
    rule_version_cleaning = as.character(rule_version_cleaning)[1],
    rule_version_taxonomy = as.character(rule_version_taxonomy)[1],
    rule_version_conversion = as.character(rule_version_conversion)[1],
    rows_in = as.integer(rows_in),
    rows_out = as.integer(rows_out),
    matched_count = as.integer(matched_count),
    unmatched_count = as.integer(unmatched_count),
    idempotence_passed = idempotence_passed,
    validation_passed = validation_passed,
    status = status,
    messages = messages
  )

  return(diagnostics)
}

#' @title persist diagnostics artifact as csv
#' @description write one diagnostics csv file under `data/audit/<dataset>/`.
#' @param diagnostics diagnostics list from `build_layer_diagnostics()`.
#' @param config named config list containing audit path fields.
#' @return character scalar output file path.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
#' @importFrom readr write_csv
#' @examples
#' \dontrun{persist_layer_diagnostics(build_layer_diagnostics("cleaning", rows_in = 1L, rows_out = 1L), config)}
persist_layer_diagnostics <- function(diagnostics, config) {
  checkmate::assert_list(diagnostics, min.len = 1)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$audit$audit_dir, min.chars = 1)

  audit_dir <- config$paths$data$audit$audit_dir
  fs::dir_create(audit_dir, recurse = TRUE)

  file_name <- paste0(
    diagnostics$layer_name,
    "_diagnostics_",
    format(Sys.time(), "%Y%m%d_%H%M%S", tz = "UTC"),
    ".csv"
  )

  output_path <- fs::path(audit_dir, file_name)

  diagnostics_dt <- data.table::data.table(
    layer_name = diagnostics$layer_name,
    execution_timestamp_utc = diagnostics$execution_timestamp_utc,
    rule_version_cleaning = diagnostics$rule_version_cleaning,
    rule_version_taxonomy = diagnostics$rule_version_taxonomy,
    rule_version_conversion = diagnostics$rule_version_conversion,
    rows_in = diagnostics$rows_in,
    rows_out = diagnostics$rows_out,
    matched_count = diagnostics$matched_count,
    unmatched_count = diagnostics$unmatched_count,
    idempotence_passed = diagnostics$idempotence_passed,
    validation_passed = diagnostics$validation_passed,
    status = diagnostics$status,
    messages = paste(diagnostics$messages, collapse = " | ")
  )

  readr::write_csv(diagnostics_dt, output_path)

  return(output_path)
}

#' @title read rule table from csv or xlsx
#' @description load a mapping table from a file path using extension-specific
#' readers.
#' @param file_path character scalar path to rule file.
#' @return data.table rule table.
#' @importFrom checkmate assert_string assert_file_exists
#' @importFrom fs path_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @examples
#' \dontrun{read_rule_table("data/imports/cleaning imports/cleaning_rules.csv")}
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  extension <- fs::path_ext(file_path) |>
    tolower()

  if (extension == "csv") {
    rules_dt <- readr::read_csv(file_path, show_col_types = FALSE)
    return(data.table::as.data.table(rules_dt))
  }

  if (extension %in% c("xlsx", "xls")) {
    rules_dt <- readxl::read_excel(file_path)
    return(data.table::as.data.table(rules_dt))
  }

  cli::cli_abort("unsupported rule file extension for {.file {file_path}}")
}

#' @title validate required rule schema
#' @description enforce required columns and missingness checks for rule tables.
#' @param rules_dt data.table rule table.
#' @param required_columns character vector of required columns.
#' @param layer_name character scalar layer name for messages.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character assert_string
#' @examples
#' validate_rule_schema(data.table::data.table(a = 1), "a", "example")
validate_rule_schema <- function(rules_dt, required_columns, layer_name) {
  checkmate::assert_data_frame(rules_dt, min.rows = 1)
  checkmate::assert_character(
    required_columns,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_string(layer_name, min.chars = 1)

  missing_columns <- setdiff(required_columns, colnames(rules_dt))

  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "{layer_name} rule schema is missing required columns",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  required_subset <- rules_dt[, ..required_columns]

  for (column_name in required_columns) {
    if (any(is.na(required_subset[[column_name]]))) {
      cli::cli_abort(
        "{layer_name} rule table contains missing values in required column {.val {column_name}}"
      )
    }
  }

  return(invisible(TRUE))
}

#' @title load cleaning rules
#' @description discover and load one cleaning mapping file from configured
#' cleaning imports directory.
#' @param config named configuration list.
#' @return named list with `rules_dt`, `template_created`, and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' \dontrun{load_cleaning_rules(config)}
load_cleaning_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$cleaning, min.chars = 1)

  cleaning_dir <- config$paths$data$imports$cleaning

  fs::dir_create(cleaning_dir, recurse = TRUE)

  candidate_files <- fs::dir_ls(
    cleaning_dir,
    regexp = "\\.(csv|xlsx|xls)$",
    type = "file",
    recurse = FALSE
  )

  template_created <- FALSE

  if (length(candidate_files) == 0) {
    template_path <- fs::path(
      cleaning_dir,
      paste0(
        "cleaning_mapping",
        ".xlsx"
      )
    )

    cleaning_template_dt <- data.table::data.table(
      target_column = character(0),
      original_value = character(0),
      cleaned_value = character(0)
    )

    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "cleaning_mapping")
    openxlsx::writeData(wb, "cleaning_mapping", cleaning_template_dt)
    openxlsx::saveWorkbook(wb, template_path, overwrite = FALSE)

    cli::cli_alert_info(
      "created cleaning template: {.file {basename(template_path)}}"
    )

    candidate_files <- template_path
    template_created <- TRUE
  }

  selected_file <- sort(as.character(candidate_files))[1]

  return(list(
    rules_dt = read_rule_table(selected_file),
    template_created = template_created,
    source_path = selected_file
  ))
}

#' @title validate cleaning rules
#' @description validate schema, active-key uniqueness, and target column
#' availability for cleaning mappings.
#' @param rules_dt data.table cleaning rules.
#' @param target_columns character vector of dataset columns to clean.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character
#' @examples
#' \dontrun{validate_cleaning_rules(rules_dt, c("country", "unit"))}
validate_cleaning_rules <- function(rules_dt, target_columns) {
  checkmate::assert_data_frame(rules_dt, min.rows = 1)
  checkmate::assert_character(target_columns, min.len = 1, any.missing = FALSE)

  required_columns <- c(
    "target_column",
    "original_value",
    "cleaned_value",
    "rule_version",
    "active_flag"
  )
  validate_rule_schema(rules_dt, required_columns, "cleaning")

  active_rules <- data.table::as.data.table(rules_dt)

  if (nrow(active_rules) == 0) {
    cli::cli_abort("cleaning rules contain no active rows")
  }

  duplicate_keys <- rules_dt[, .N, by = .(target_column, original_value)][N > 1]

  if (nrow(duplicate_keys) > 0) {
    cli::cli_abort("cleaning rules contain duplicate active keys")
  }

  unknown_targets <- setdiff(unique(active_rules$target_column), target_columns)

  if (length(unknown_targets) > 0) {
    cli::cli_warn(c(
      "cleaning rules contain target columns not present in requested targets",
      "i" = paste(unknown_targets, collapse = ", ")
    ))
  }

  return(invisible(TRUE))
}

#' @title apply cleaning rules
#' @description apply deterministic one-to-one cleaning mappings for each target
#' column using normalized join keys.
#' @param dataset_dt input data.table.
#' @param rules_dt cleaning rules data.table.
#' @param key_columns optional key columns preserved for context.
#' @return named list with cleaned data, matched count, and unmatched count.
#' @importFrom checkmate assert_data_frame assert_character
#' @examples
#' \dontrun{apply_cleaning_rules(dataset_dt, rules_dt, c("document"))}
apply_cleaning_rules <- function(
  dataset_dt,
  rules_dt,
  key_columns = character()
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_data_frame(rules_dt, min.rows = 1)
  checkmate::assert_character(key_columns, any.missing = FALSE)

  cleaned_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  active_rules <- data.table::as.data.table(rules_dt)[as.logical(active_flag)]

  matched_count <- 0L
  unmatched_count <- 0L

  target_columns <- intersect(
    unique(active_rules$target_column),
    colnames(cleaned_dt)
  )

  for (target_column in target_columns) {
    column_rules <- active_rules[target_column == ..target_column]

    column_rules[, original_key := normalize_string(original_value)]
    data.table::setkey(column_rules, original_key)

    source_values <- cleaned_dt[[target_column]]
    source_keys <- normalize_string(source_values)

    matched_index <- column_rules[source_keys, which = TRUE]
    is_matched <- !is.na(matched_index)

    matched_count <- matched_count + as.integer(sum(is_matched))

    unmatched_unique <- unique(source_keys[!is_matched & source_keys != ""])
    unmatched_count <- unmatched_count + length(unmatched_unique)

    if (any(is_matched)) {
      replacement_values <- column_rules$cleaned_value[matched_index[
        is_matched
      ]]
      cleaned_dt[is_matched, (target_column) := replacement_values]
    }
  }

  return(list(
    data = cleaned_dt,
    matched_count = as.integer(matched_count),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title run cleaning layer
#' @description execute rule-driven cleaning with schema checks, idempotency,
#' standardized diagnostics, and diagnostics persistence.
#' @param dataset_dt input data.frame/data.table.
#' @param config named configuration list.
#' @return cleaned data.table with diagnostics in attributes.
#' @importFrom checkmate assert_data_frame assert_list assert_flag assert_choice
#' @examples
#' \dontrun{run_cleaning_layer(fao_data_raw, config)}
run_cleaning_layer <- function(dataset_dt, config) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  cleaned_dt <- data.table::as.data.table(data.table::copy(dataset_dt))
  rows_in <- as.integer(nrow(cleaned_dt))

  cleaning_rules_payload <- load_cleaning_rules(config)
  checkmate::assert_names(
    names(cleaning_rules_payload),
    must.include = c("rules_dt", "template_created", "source_path")
  )
  checkmate::assert_data_frame(cleaning_rules_payload$rules_dt, min.rows = 0)
  checkmate::assert_flag(cleaning_rules_payload$template_created)
  checkmate::assert_string(cleaning_rules_payload$source_path, min.chars = 1)

  cleaning_rules <- cleaning_rules_payload$rules_dt

  active_cleaning_rows <- !is.na(as.logical(cleaning_rules$active_flag)) &
    as.logical(cleaning_rules$active_flag)

  if (nrow(cleaning_rules) == 0 || !any(active_cleaning_rows)) {
    diagnostics <- build_layer_diagnostics(
      layer_name = "cleaning",
      rows_in = rows_in,
      rows_out = rows_in,
      matched_count = 0L,
      unmatched_count = 0L,
      idempotence_passed = TRUE,
      validation_passed = TRUE,
      status = "warn",
      messages = "step skipped: no active rules in template"
    )

    diagnostics_path <- persist_layer_diagnostics(diagnostics, config)
    attr(cleaned_dt, "layer_diagnostics") <- diagnostics
    attr(cleaned_dt, "layer_diagnostics_path") <- diagnostics_path

    return(cleaned_dt)
  }

  validate_cleaning_rules(cleaning_rules, colnames(cleaned_dt))

  cleaning_rule_version <- unique(as.character(cleaning_rules$rule_version))[1]

  cleaning_result <- apply_cleaning_rules(cleaned_dt, cleaning_rules)
  cleaned_dt <- cleaning_result$data

  rows_out <- as.integer(nrow(cleaned_dt))
  idempotence_passed <- identical(
    cleaned_dt,
    apply_cleaning_rules(cleaned_dt, cleaning_rules)$data
  )

  unmatched_policy <- purrr::pluck(
    config,
    "cleaning_unmatched_policy",
    .default = "report"
  )

  checkmate::assert_choice(unmatched_policy, c("report", "fail"))

  if (unmatched_policy == "fail" && cleaning_result$unmatched_count > 0) {
    cli::cli_abort("cleaning unmatched_count is non-zero under fail policy")
  }

  diagnostics_status <- if (cleaning_result$unmatched_count > 0) {
    "warn"
  } else {
    "pass"
  }

  diagnostics <- build_layer_diagnostics(
    layer_name = "cleaning",
    rows_in = rows_in,
    rows_out = rows_out,
    matched_count = cleaning_result$matched_count,
    unmatched_count = cleaning_result$unmatched_count,
    idempotence_passed = idempotence_passed,
    validation_passed = (rows_in == rows_out),
    status = diagnostics_status,
    messages = character(0)
  )

  diagnostics_path <- persist_layer_diagnostics(diagnostics, config)

  attr(cleaned_dt, "layer_diagnostics") <- diagnostics
  attr(cleaned_dt, "layer_diagnostics_path") <- diagnostics_path

  return(cleaned_dt)
}

#' @title load harmonization rules
#' @description load taxonomy and conversion rule tables from harmonization
#' imports folder.
#' @param config named configuration list.
#' @return named list with `taxonomy_rules`, `conversion_rules`,
#' `template_created`, `taxonomy_source_path`, and `conversion_source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' \dontrun{load_harmonization_rules(config)}
load_harmonization_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(
    config$paths$data$imports$harmonization,
    min.chars = 1
  )

  harmonization_dir <- config$paths$data$imports$harmonization

  fs::dir_create(harmonization_dir, recurse = TRUE)

  candidate_files <- fs::dir_ls(
    harmonization_dir,
    regexp = "\\.(csv|xlsx|xls)$",
    type = "file",
    recurse = FALSE
  )

  template_created <- FALSE

  if (length(candidate_files) == 0) {
    taxonomy_template_path <- fs::path(
      harmonization_dir,
      paste0("taxonomy_mapping", ".xlsx")
    )
    conversion_template_path <- fs::path(
      harmonization_dir,
      paste0("conversion_mapping", ".xlsx")
    )

    taxonomy_template_dt <- data.table::data.table(
      entity_key = character(0),
      canonical_entity = character(0),
      taxonomy_code = character(0),
      hierarchy_level = character(0),
      rule_version = character(0),
      active_flag = logical(0)
    )
    conversion_template_dt <- data.table::data.table(
      from_unit = character(0),
      to_unit = character(0),
      factor = numeric(0),
      offset = numeric(0),
      rule_version = character(0),
      active_flag = logical(0)
    )

    taxonomy_wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(taxonomy_wb, "taxonomy_rules")
    openxlsx::writeData(taxonomy_wb, "taxonomy_rules", taxonomy_template_dt)
    openxlsx::saveWorkbook(
      taxonomy_wb,
      taxonomy_template_path,
      overwrite = FALSE
    )

    conversion_wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(conversion_wb, "conversion_rules")
    openxlsx::writeData(
      conversion_wb,
      "conversion_rules",
      conversion_template_dt
    )
    openxlsx::saveWorkbook(
      conversion_wb,
      conversion_template_path,
      overwrite = FALSE
    )

    cli::cli_alert_info(
      "created harmonization templates: {.file {basename(taxonomy_template_path)}} and {.file {basename(conversion_template_path)}}"
    )

    candidate_files <- c(taxonomy_template_path, conversion_template_path)
    template_created <- TRUE
  }

  file_names <- basename(candidate_files) |>
    tolower()

  taxonomy_index <- grep("taxonomy", file_names)[1]
  conversion_index <- grep("conversion|unit", file_names)[1]

  if (is.na(taxonomy_index) || is.na(conversion_index)) {
    cli::cli_abort(
      "could not identify taxonomy and conversion rule files in harmonization imports"
    )
  }

  taxonomy_rules <- read_rule_table(candidate_files[taxonomy_index])
  conversion_rules <- read_rule_table(candidate_files[conversion_index])

  return(list(
    taxonomy_rules = taxonomy_rules,
    conversion_rules = conversion_rules,
    template_created = template_created,
    taxonomy_source_path = candidate_files[taxonomy_index],
    conversion_source_path = candidate_files[conversion_index]
  ))
}

#' @title validate taxonomy rules
#' @description validate taxonomy rule table schema and active-key uniqueness.
#' @param taxonomy_dt taxonomy rules data.table.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{validate_taxonomy_rules(taxonomy_dt)}
validate_taxonomy_rules <- function(taxonomy_dt) {
  checkmate::assert_data_frame(taxonomy_dt, min.rows = 1)

  required_columns <- c(
    "entity_key",
    "canonical_entity",
    "taxonomy_code",
    "hierarchy_level",
    "rule_version",
    "active_flag"
  )

  validate_rule_schema(taxonomy_dt, required_columns, "harmonization taxonomy")

  active_taxonomy <- taxonomy_dt[as.logical(active_flag)]

  duplicate_active <- active_taxonomy[, .N, by = .(entity_key, rule_version)][
    N > 1
  ]

  if (nrow(duplicate_active) > 0) {
    cli::cli_abort("taxonomy rules contain duplicate active entity keys")
  }

  return(invisible(TRUE))
}

#' @title validate conversion rules
#' @description validate numeric conversion rule schema and active uniqueness.
#' @param conversion_dt conversion rules data.table.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{validate_conversion_rules(conversion_dt)}
validate_conversion_rules <- function(conversion_dt) {
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)

  required_columns <- c(
    "from_unit",
    "to_unit",
    "factor",
    "offset",
    "rule_version",
    "active_flag"
  )

  validate_rule_schema(
    conversion_dt,
    required_columns,
    "harmonization conversion"
  )

  active_conversion <- conversion_dt[as.logical(active_flag)]

  duplicate_active <- active_conversion[,
    .N,
    by = .(from_unit, to_unit, rule_version)
  ][N > 1]

  if (nrow(duplicate_active) > 0) {
    cli::cli_abort("conversion rules contain duplicate active unit pairs")
  }

  if (any(!is.finite(as.numeric(active_conversion$factor)))) {
    cli::cli_abort("conversion factor values must be finite")
  }

  return(invisible(TRUE))
}

#' @title apply taxonomy mapping
#' @description map cleaned entity key to canonical taxonomy fields.
#' @param cleaned_dt cleaned data.table.
#' @param taxonomy_dt taxonomy rules data.table.
#' @param entity_column character scalar entity key column name in data.
#' @return named list with harmonized data, matched count, unmatched count.
#' @importFrom checkmate assert_data_frame assert_string
#' @examples
#' \dontrun{apply_taxonomy_mapping(cleaned_dt, taxonomy_dt, "country")}
apply_taxonomy_mapping <- function(cleaned_dt, taxonomy_dt, entity_column) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_data_frame(taxonomy_dt, min.rows = 1)
  checkmate::assert_string(entity_column, min.chars = 1)

  if (!entity_column %in% colnames(cleaned_dt)) {
    cli::cli_abort(
      "entity column {.val {entity_column}} is missing from cleaned data"
    )
  }

  mapped_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  active_taxonomy <- data.table::as.data.table(taxonomy_dt)[as.logical(
    active_flag
  )]

  active_taxonomy[, entity_key_normalized := normalize_string(entity_key)]
  data.table::setkey(active_taxonomy, entity_key_normalized)

  data_entity_key <- normalize_string(mapped_dt[[entity_column]])
  matched_index <- active_taxonomy[data_entity_key, which = TRUE]
  is_matched <- !is.na(matched_index)

  mapped_dt[, canonical_entity := NA_character_]
  mapped_dt[, taxonomy_code := NA_character_]
  mapped_dt[, hierarchy_level := NA_character_]

  if (any(is_matched)) {
    mapped_dt[
      is_matched,
      canonical_entity := active_taxonomy$canonical_entity[matched_index[
        is_matched
      ]]
    ]
    mapped_dt[
      is_matched,
      taxonomy_code := active_taxonomy$taxonomy_code[matched_index[is_matched]]
    ]
    mapped_dt[
      is_matched,
      hierarchy_level := as.character(active_taxonomy$hierarchy_level[matched_index[
        is_matched
      ]])
    ]
  }

  unmatched_count <- length(unique(data_entity_key[
    !is_matched & data_entity_key != ""
  ]))

  return(list(
    data = mapped_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title apply numeric unit harmonization
#' @description convert numeric values to standardized units using deterministic
#' conversion factors and offsets.
#' @param mapped_dt mapped data.table.
#' @param conversion_dt conversion rules data.table.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @return named list with transformed data, matched count, unmatched count.
#' @importFrom checkmate assert_data_frame assert_string
#' @examples
#' \dontrun{apply_numeric_harmonization(mapped_dt, conversion_dt, "unit", "value")}
apply_numeric_harmonization <- function(
  mapped_dt,
  conversion_dt,
  unit_column,
  value_column
) {
  checkmate::assert_data_frame(mapped_dt, min.rows = 0)
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)

  if (!unit_column %in% colnames(mapped_dt)) {
    cli::cli_abort("unit column {.val {unit_column}} is missing from data")
  }

  if (!value_column %in% colnames(mapped_dt)) {
    cli::cli_abort("value column {.val {value_column}} is missing from data")
  }

  harmonized_dt <- data.table::copy(data.table::as.data.table(mapped_dt))
  active_conversion <- data.table::as.data.table(conversion_dt)[as.logical(
    active_flag
  )]

  active_conversion[, from_unit_normalized := normalize_string(from_unit)]
  data.table::setkey(active_conversion, from_unit_normalized)

  source_units <- normalize_string(harmonized_dt[[unit_column]])
  matched_index <- active_conversion[source_units, which = TRUE]
  is_matched <- !is.na(matched_index)

  numeric_values <- suppressWarnings(as.numeric(harmonized_dt[[value_column]]))

  if (any(is.na(numeric_values) & !is.na(harmonized_dt[[value_column]]))) {
    cli::cli_abort(
      "value column contains non-numeric values that cannot be harmonized"
    )
  }

  if (any(is_matched)) {
    matched_factors <- as.numeric(active_conversion$factor[matched_index[
      is_matched
    ]])
    matched_offsets <- as.numeric(active_conversion$offset[matched_index[
      is_matched
    ]])

    numeric_values[is_matched] <- numeric_values[is_matched] *
      matched_factors +
      matched_offsets
    harmonized_dt[
      is_matched,
      (unit_column) := active_conversion$to_unit[matched_index[is_matched]]
    ]
  }

  harmonized_dt[, (value_column) := numeric_values]

  unmatched_count <- length(unique(source_units[
    !is_matched & source_units != ""
  ]))

  return(list(
    data = harmonized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title run harmonization layer
#' @description execute taxonomy and numeric harmonization with idempotency and
#' diagnostics persistence.
#' @param cleaned_dt cleaned data.frame/data.table.
#' @param config named configuration list.
#' @return harmonized data.table with diagnostics attributes.
#' @importFrom checkmate assert_data_frame assert_list
#' @examples
#' \dontrun{run_harmonization_layer(cleaned_dt, config)}
run_harmonization_layer <- function(cleaned_dt, config) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  harmonized_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  rows_in <- as.integer(nrow(harmonized_dt))

  harmonization_rules <- load_harmonization_rules(config)
  checkmate::assert_names(
    names(harmonization_rules),
    must.include = c(
      "taxonomy_rules",
      "conversion_rules",
      "template_created",
      "taxonomy_source_path",
      "conversion_source_path"
    )
  )
  checkmate::assert_data_frame(harmonization_rules$taxonomy_rules, min.rows = 0)
  checkmate::assert_data_frame(
    harmonization_rules$conversion_rules,
    min.rows = 0
  )
  checkmate::assert_flag(harmonization_rules$template_created)
  checkmate::assert_string(
    harmonization_rules$taxonomy_source_path,
    min.chars = 1
  )
  checkmate::assert_string(
    harmonization_rules$conversion_source_path,
    min.chars = 1
  )

  taxonomy_rules <- harmonization_rules$taxonomy_rules
  conversion_rules <- harmonization_rules$conversion_rules

  taxonomy_active_rows <- !is.na(as.logical(taxonomy_rules$active_flag)) &
    as.logical(taxonomy_rules$active_flag)
  conversion_active_rows <- !is.na(as.logical(conversion_rules$active_flag)) &
    as.logical(conversion_rules$active_flag)

  if (
    nrow(taxonomy_rules) == 0 ||
      nrow(conversion_rules) == 0 ||
      !any(taxonomy_active_rows) ||
      !any(conversion_active_rows)
  ) {
    diagnostics <- build_layer_diagnostics(
      layer_name = "harmonization",
      rows_in = rows_in,
      rows_out = rows_in,
      matched_count = 0L,
      unmatched_count = 0L,
      idempotence_passed = TRUE,
      validation_passed = TRUE,
      status = "warn",
      messages = "step skipped: no active rules in template"
    )

    diagnostics_path <- persist_layer_diagnostics(diagnostics, config)
    attr(harmonized_dt, "layer_diagnostics") <- diagnostics
    attr(harmonized_dt, "layer_diagnostics_path") <- diagnostics_path

    return(harmonized_dt)
  }

  validate_taxonomy_rules(taxonomy_rules)
  validate_conversion_rules(conversion_rules)

  taxonomy_rule_version <- unique(as.character(taxonomy_rules$rule_version))[1]
  conversion_rule_version <- unique(as.character(
    conversion_rules$rule_version
  ))[1]

  already_harmonized <-
    "_harmonized_flag" %in%
    colnames(harmonized_dt) &&
    "_harmonization_rule_version" %in% colnames(harmonized_dt) &&
    all(isTRUE(harmonized_dt[["_harmonized_flag"]])) &&
    all(
      harmonized_dt[["_harmonization_rule_version"]] ==
        paste(taxonomy_rule_version, conversion_rule_version, sep = "|")
    )

  if (already_harmonized) {
    diagnostics <- build_layer_diagnostics(
      layer_name = "harmonization",
      rule_version_taxonomy = taxonomy_rule_version,
      rule_version_conversion = conversion_rule_version,
      rows_in = rows_in,
      rows_out = rows_in,
      matched_count = 0L,
      unmatched_count = 0L,
      idempotence_passed = TRUE,
      validation_passed = TRUE,
      status = "warn",
      messages = "harmonization skipped because identical rule versions are already applied"
    )

    diagnostics_path <- persist_layer_diagnostics(diagnostics, config)
    attr(harmonized_dt, "layer_diagnostics") <- diagnostics
    attr(harmonized_dt, "layer_diagnostics_path") <- diagnostics_path

    return(harmonized_dt)
  }

  entity_column <- purrr::pluck(
    config,
    "harmonization",
    "entity_column",
    .default = "country"
  )
  unit_column <- purrr::pluck(
    config,
    "harmonization",
    "unit_column",
    .default = "unit"
  )
  value_column <- purrr::pluck(
    config,
    "harmonization",
    "value_column",
    .default = "value"
  )

  taxonomy_result <- apply_taxonomy_mapping(
    harmonized_dt,
    taxonomy_rules,
    entity_column
  )
  harmonized_dt <- taxonomy_result$data

  conversion_result <- apply_numeric_harmonization(
    harmonized_dt,
    conversion_rules,
    unit_column,
    value_column
  )
  harmonized_dt <- conversion_result$data

  harmonized_dt[, `_harmonized_flag` := TRUE]
  harmonized_dt[,
    `_harmonization_rule_version` := paste(
      taxonomy_rule_version,
      conversion_rule_version,
      sep = "|"
    )
  ]

  unmatched_total <- taxonomy_result$unmatched_count +
    conversion_result$unmatched_count

  unmatched_policy <- purrr::pluck(
    config,
    "harmonization_unmatched_policy",
    .default = "fail"
  )

  checkmate::assert_choice(unmatched_policy, c("report", "fail"))

  if (unmatched_policy == "fail" && unmatched_total > 0) {
    cli::cli_abort(
      "harmonization unmatched_count is non-zero under fail policy"
    )
  }

  rows_out <- as.integer(nrow(harmonized_dt))

  diagnostics <- build_layer_diagnostics(
    layer_name = "harmonization",
    rule_version_taxonomy = taxonomy_rule_version,
    rule_version_conversion = conversion_rule_version,
    rows_in = rows_in,
    rows_out = rows_out,
    matched_count = taxonomy_result$matched_count +
      conversion_result$matched_count,
    unmatched_count = unmatched_total,
    idempotence_passed = TRUE,
    validation_passed = TRUE,
    status = if (unmatched_total > 0) "warn" else "pass",
    messages = character(0)
  )

  diagnostics_path <- persist_layer_diagnostics(diagnostics, config)

  attr(harmonized_dt, "layer_diagnostics") <- diagnostics
  attr(harmonized_dt, "layer_diagnostics_path") <- diagnostics_path

  return(harmonized_dt)
}

#' @title run cleaning and harmonization pipeline
#' @description orchestrate cleaning and harmonization, collecting diagnostics
#' for each layer and returning the final dataset.
#' @param raw_dt raw data.frame/data.table.
#' @param config named configuration list.
#' @return final data.table with `pipeline_diagnostics` and
#' `pipeline_diagnostics_paths` attributes.
#' @importFrom checkmate assert_data_frame assert_list assert_flag
#' @examples
#' \dontrun{run_clean_harmonize_pipeline(fao_data_raw, config)}
run_clean_harmonize_pipeline <- function(raw_dt, config) {
  checkmate::assert_data_frame(raw_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  cleaned_dt <- run_cleaning_layer(raw_dt, config)
  cleaning_diagnostics <- attr(cleaned_dt, "layer_diagnostics", exact = TRUE)
  cleaning_path <- attr(cleaned_dt, "layer_diagnostics_path", exact = TRUE)

  harmonized_dt <- run_harmonization_layer(cleaned_dt, config)
  harmonization_diagnostics <- attr(
    harmonized_dt,
    "layer_diagnostics",
    exact = TRUE
  )
  harmonization_path <- attr(
    harmonized_dt,
    "layer_diagnostics_path",
    exact = TRUE
  )

  final_dt <- harmonized_dt

  all_diagnostics <- list(
    cleaning = cleaning_diagnostics,
    harmonization = harmonization_diagnostics
  )

  if (
    any(vapply(
      all_diagnostics,
      function(x) identical(x$status, "fail"),
      logical(1)
    ))
  ) {
    cli::cli_abort(
      "cleaning and harmonization pipeline aborted due to failed diagnostics status"
    )
  }

  attr(final_dt, "pipeline_diagnostics") <- all_diagnostics
  attr(final_dt, "pipeline_diagnostics_paths") <- list(
    cleaning = cleaning_path,
    harmonization = harmonization_path
  )

  return(final_dt)
}
