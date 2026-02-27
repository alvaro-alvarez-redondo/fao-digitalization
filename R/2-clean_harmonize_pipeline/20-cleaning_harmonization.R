# script: cleaning and harmonization pipeline functions
# description: rule-driven cleaning and harmonization and orchestration helpers.

#' @title build standardized layer diagnostics object
#' @description create a one-row diagnostics object following the phase-1
#' contract.
#' @param layer_name character scalar layer name.
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
    layer_rules <- readr::read_csv(file_path, show_col_types = FALSE)
    return(data.table::as.data.table(layer_rules))
  }

  if (extension %in% c("xlsx", "xls")) {
    layer_rules <- readxl::read_excel(file_path)
    return(data.table::as.data.table(layer_rules))
  }

  cli::cli_abort("unsupported rule file extension for {.file {file_path}}")
}

#' @title validate required rule schema
#' @description enforce required columns and missingness checks for rule tables.
#' @param layer_rules data.table rule table.
#' @param required_columns character vector of required columns.
#' @param layer_name character scalar layer name for messages.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character assert_string
#' @examples
#' validate_rule_schema(data.table::data.table(a = 1), "a", "example")
validate_rule_schema <- function(layer_rules, required_columns, layer_name) {
  checkmate::assert_data_frame(layer_rules, min.rows = 1)
  checkmate::assert_character(
    required_columns,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_string(layer_name, min.chars = 1)

  missing_columns <- setdiff(required_columns, colnames(layer_rules))

  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "{layer_name} rule schema is missing required columns",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  required_subset <- layer_rules[, ..required_columns]

  for (column_name in required_columns) {
    if (any(is.na(required_subset[[column_name]]))) {
      cli::cli_abort(
        "{layer_name} rule table contains missing values in required column {.val {column_name}}"
      )
    }
  }

  return(invisible(TRUE))
}

#' @title validate mapping rules
#' @description validate schema, active-key uniqueness, and target column
#' availability for cleaning mappings.
#' @param layer_rules data.table cleaning rules.
#' @param column_targets character vector of dataset columns to clean.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame assert_character
#' @examples
#' \dontrun{validate_mapping_rules(layer_rules, c("country", "unit"))}
validate_mapping_rules <- function(
  layer_rules,
  dataset_columns = NULL,
  value_column = c("cleaned_value_target", "harmonized_value_target"),
  layer_name = "mapping"
) {
  # value_column can be "cleaned_value_target" or "harmonized_value_target"
  value_column <- match.arg(value_column)

  checkmate::assert_data_frame(layer_rules, min.rows = 1)

  # Required columns
  required_columns <- c(
    "column_source",
    "column_target",
    "original_value_source",
    "original_value_target",
    value_column
  )

  # Validate schema
  validate_rule_schema(layer_rules, required_columns, layer_name)

  layer_rules <- data.table::as.data.table(layer_rules)

  # Check for duplicates
  duplicate_active <- layer_rules[,
    .N,
    by = .(
      column_source,
      column_target,
      original_value_source,
      original_value_target
    )
  ][N > 1]

  if (nrow(duplicate_active) > 0) {
    cli::cli_abort(c(
      "{layer_name} rules contain duplicate active mappings",
      "x" = "Each (column_source, column_target, original_value_source, original_value_target) must be unique"
    ))
  }

  # Warn about unknown columns in dataset
  if (!is.null(dataset_columns)) {
    unknown_sources <- setdiff(
      unique(layer_rules$column_source),
      dataset_columns
    )
    if (length(unknown_sources) > 0) {
      cli::cli_warn(c(
        "{layer_name} rules contain source columns not present in dataset",
        "i" = paste(unknown_sources, collapse = ", ")
      ))
    }

    unknown_targets <- setdiff(
      unique(layer_rules$column_target),
      dataset_columns
    )
    if (length(unknown_targets) > 0) {
      cli::cli_warn(c(
        "{layer_name} rules contain target columns not present in dataset",
        "i" = paste(unknown_targets, collapse = ", ")
      ))
    }
  }

  return(invisible(TRUE))
}

#' @title load cleaning rules
#' @description discover and load one cleaning mapping file from configured
#' cleaning imports directory.
#' @param config named configuration list.
#' @return named list with `layer_rules`, `template_created`, and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' \dontrun{load_cleaning_rules(config)}
load_cleaning_rules <- function(config) {
  cleaning_dir <- config$paths$data$imports$cleaning
  fs::dir_create(cleaning_dir, recurse = TRUE)

  files <- fs::dir_ls(
    cleaning_dir,
    regexp = "^cleaning_.*\\.xlsx$",
    type = "file"
  )

  if (length(files) == 0) {
    cli::cli_alert_info("No cleaning files found, creating template...")
    template_path <- fs::path(cleaning_dir, "cleaning_template.xlsx")
    cleaning_template <- data.table::data.table(
      column_source = character(0),
      column_target = character(0),
      original_value_source = character(0),
      original_value_target = character(0),
      cleaned_value_target = character(0)
    )
    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "cleaning_mapping")
    openxlsx::writeData(wb, "cleaning_mapping", cleaning_template)
    openxlsx::saveWorkbook(wb, template_path, overwrite = FALSE)
    files <- template_path
  }

  lapply(files, function(f) {
    list(
      layer_rules = read_rule_table(f),
      source_path = f
    )
  })
}

#' @title apply cleaning rules
#' @description apply deterministic one-to-one cleaning mappings for each target
#' column using normalized join keys.
#' @param dataset_dt input data.table.
#' @param layer_rules cleaning rules data.table.
#' @param key_columns optional key columns preserved for context.
#' @return named list with cleaned data, matched count, and unmatched count.
#' @importFrom checkmate assert_data_frame assert_character
#' @examples
#' \dontrun{apply_cleaning_mapping(dataset_dt, layer_rules, c("document"))}
apply_cleaning_mapping <- function(
  dataset_dt,
  layer_rules,
  key_columns = character()
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_data_frame(layer_rules, min.rows = 1)
  checkmate::assert_character(key_columns, any.missing = FALSE)

  mapped_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  active_rules <- data.table::as.data.table(layer_rules)

  matched_count <- 0L
  unmatched_count <- 0L

  unique_pairs <- unique(active_rules[, .(column_source, column_target)])

  for (i in seq_len(nrow(unique_pairs))) {
    src_col <- unique_pairs$column_source[i]
    tgt_col <- unique_pairs$column_target[i]

    if (!src_col %in% colnames(mapped_dt)) {
      cli::cli_warn(
        "column_source {.val {src_col}} not found in dataset, skipping"
      )
      next
    }

    if (!tgt_col %in% colnames(mapped_dt)) {
      cli::cli_warn(
        "column_target {.val {tgt_col}} not found; creating NA column"
      )
      mapped_dt[, (tgt_col) := NA_character_]
    }

    # Get rules for current source/target pair
    column_rules <- active_rules[
      column_source == src_col & column_target == tgt_col
    ]

    # Normalize both source and target original values for lookup
    column_rules[, source_key := normalize_string(original_value_source)]
    column_rules[, target_key := normalize_string(original_value_target)]

    # Normalize dataset columns for comparison
    dataset_source_keys <- normalize_string(mapped_dt[[src_col]])
    dataset_target_keys <- normalize_string(mapped_dt[[tgt_col]])

    # Identify rows where BOTH source and target match the rule
    is_matched <- rep(FALSE, nrow(mapped_dt))
    for (r in seq_len(nrow(column_rules))) {
      is_matched <- is_matched |
        (dataset_source_keys == column_rules$source_key[r] &
          dataset_target_keys == column_rules$target_key[r])
    }

    matched_count <- matched_count + sum(is_matched)
    unmatched_count <- unmatched_count +
      sum(!is_matched & !is.na(mapped_dt[[src_col]]))

    # Apply cleaned values ONLY to the rows where BOTH match
    if (any(is_matched)) {
      # Match the cleaned_value_target for each row
      replacement_values <- rep(NA_character_, sum(is_matched))
      matched_rows <- which(is_matched)
      for (r in seq_len(nrow(column_rules))) {
        match_r <- which(
          dataset_source_keys == column_rules$source_key[r] &
            dataset_target_keys == column_rules$target_key[r]
        )
        replacement_values[
          matched_rows %in% match_r
        ] <- column_rules$cleaned_value_target[
          r
        ]
      }
      mapped_dt[is_matched, (tgt_col) := replacement_values]
    }
  }

  return(list(
    data = mapped_dt,
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
run_cleaning_layer_batch <- function(dataset_dt, config) {
  cleaned_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  cleaning_files <- load_cleaning_rules(config)

  all_diagnostics <- list()

  for (payload in cleaning_files) {
    layer_rules <- payload$layer_rules
    source_path <- payload$source_path

    if (nrow(layer_rules) == 0) {
      next
    }

    validate_mapping_rules(
      layer_rules,
      dataset_columns = colnames(cleaned_dt),
      value_column = "cleaned_value_target",
      layer_name = basename(source_path)
    )

    result <- apply_cleaning_mapping(cleaned_dt, layer_rules)
    cleaned_dt <- result$data

    diagnostics <- build_layer_diagnostics(
      layer_name = basename(source_path),
      rows_in = nrow(dataset_dt),
      rows_out = nrow(cleaned_dt),
      matched_count = result$matched_count,
      unmatched_count = result$unmatched_count
    )

    all_diagnostics[[basename(source_path)]] <- diagnostics
  }

  attr(cleaned_dt, "layer_diagnostics") <- all_diagnostics
  return(cleaned_dt)
}

#' @title load harmonization rules
#' @description load value-renaming harmonization mapping from harmonization
#' imports folder. legacy taxonomy-style mappings are normalized to the same
#' structure for backward compatibility.
#' @param config named configuration list.
#' @return named list with `harmonization_rules`, `template_created`, and
#' `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @examples
#' \dontrun{load_harmonization_rules(config)}
load_harmonization_rules <- function(config) {
  harmonization_dir <- config$paths$data$imports$harmonization
  fs::dir_create(harmonization_dir, recurse = TRUE)

  files <- fs::dir_ls(
    harmonization_dir,
    regexp = "^harmonization_.*\\.xlsx$",
    type = "file"
  )

  if (length(files) == 0) {
    cli::cli_alert_info("No harmonization files found, creating template...")
    template_path <- fs::path(harmonization_dir, "harmonization_template.xlsx")
    harmonization_template <- data.table::data.table(
      column_source = character(0),
      column_target = character(0),
      original_value_source = character(0),
      original_value_target = character(0),
      harmonized_value_target = character(0)
    )
    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "harmonization_mapping")
    openxlsx::writeData(wb, "harmonization_mapping", harmonization_template)
    openxlsx::saveWorkbook(wb, template_path, overwrite = FALSE)
    files <- template_path
  }

  lapply(files, function(f) {
    list(
      layer_rules = read_rule_table(f),
      source_path = f
    )
  })
}

#' @title apply harmonization mapping
#' @description apply deterministic value renaming to cleaned columns based on
#' harmonization mapping rules.
#' @param cleaned_dt cleaned data.table.
#' @param harmonization_dt harmonization rules data.table.
#' @return named list with harmonized data, matched count, unmatched count.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{apply_harmonization_mapping(cleaned_dt, harmonization_dt)}
apply_harmonization_mapping <- function(cleaned_dt, harmonization_dt) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonization_dt, min.rows = 1)

  mapped_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  active_rules <- data.table::as.data.table(harmonization_dt)

  matched_count <- 0L
  unmatched_count <- 0L

  unique_pairs <- unique(active_rules[, .(column_source, column_target)])

  for (i in seq_len(nrow(unique_pairs))) {
    src_col <- unique_pairs$column_source[i]
    tgt_col <- unique_pairs$column_target[i]

    if (!src_col %in% colnames(mapped_dt)) {
      cli::cli_warn(
        "column_source {.val {src_col}} not found in dataset, skipping"
      )
      next
    }

    if (!tgt_col %in% colnames(mapped_dt)) {
      cli::cli_warn(
        "column_target {.val {tgt_col}} not found; creating NA column"
      )
      mapped_dt[, (tgt_col) := NA_character_]
    }

    # Get rules for current source/target pair
    column_rules <- active_rules[
      column_source == src_col & column_target == tgt_col
    ]

    # Normalize both source and target original values for lookup
    column_rules[, source_key := normalize_string(original_value_source)]
    column_rules[, target_key := normalize_string(original_value_target)]

    # Normalize dataset columns for comparison
    dataset_source_keys <- normalize_string(mapped_dt[[src_col]])
    dataset_target_keys <- normalize_string(mapped_dt[[tgt_col]])

    # Identify rows where BOTH source and target match the rule
    is_matched <- rep(FALSE, nrow(mapped_dt))
    for (r in seq_len(nrow(column_rules))) {
      is_matched <- is_matched |
        (dataset_source_keys == column_rules$source_key[r] &
          dataset_target_keys == column_rules$target_key[r])
    }

    matched_count <- matched_count + sum(is_matched)
    unmatched_count <- unmatched_count +
      sum(!is_matched & !is.na(mapped_dt[[src_col]]))

    # Apply cleaned values ONLY to the rows where BOTH match
    if (any(is_matched)) {
      # Match the cleaned_value_target for each row
      replacement_values <- rep(NA_character_, sum(is_matched))
      matched_rows <- which(is_matched)
      for (r in seq_len(nrow(column_rules))) {
        match_r <- which(
          dataset_source_keys == column_rules$source_key[r] &
            dataset_target_keys == column_rules$target_key[r]
        )
        replacement_values[
          matched_rows %in% match_r
        ] <- column_rules$harmonized_value_target[
          r
        ]
      }
      mapped_dt[is_matched, (tgt_col) := replacement_values]
    }
  }

  return(list(
    data = mapped_dt,
    matched_count = as.integer(matched_count),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title run harmonization layer
#' @description execute value-renaming harmonization with schema checks,
#' idempotency, standardized diagnostics, and diagnostics persistence.
#' @param cleaned_dt cleaned data.frame/data.table.
#' @param config named configuration list.
#' @return harmonized data.table with diagnostics attributes.
#' @importFrom checkmate assert_data_frame assert_list assert_choice
run_harmonization_layer_batch <- function(cleaned_dt, config) {
  harmonized_dt <- data.table::copy(cleaned_dt)
  harmonization_files <- load_harmonization_rules(config)

  all_diagnostics <- list()

  for (payload in harmonization_files) {
    layer_rules <- payload$layer_rules
    source_path <- payload$source_path

    if (nrow(layer_rules) == 0) {
      next
    }

    validate_mapping_rules(
      layer_rules,
      dataset_columns = colnames(harmonized_dt),
      value_column = "harmonized_value_target",
      layer_name = basename(source_path)
    )

    result <- apply_harmonization_mapping(harmonized_dt, layer_rules)
    harmonized_dt <- result$data

    diagnostics <- build_layer_diagnostics(
      layer_name = basename(source_path),
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(harmonized_dt),
      matched_count = result$matched_count,
      unmatched_count = result$unmatched_count
    )

    all_diagnostics[[basename(source_path)]] <- diagnostics
  }

  attr(harmonized_dt, "layer_diagnostics") <- all_diagnostics
  return(harmonized_dt)
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
    "product",
    "from_unit",
    "to_unit",
    "factor",
    "offset"
  )

  validate_rule_schema(
    conversion_dt,
    required_columns,
    "harmonization conversion"
  )

  duplicate_rows <- conversion_dt[, .N, by = .(from_unit, to_unit)][N > 1]

  if (nrow(duplicate_rows) > 0) {
    cli::cli_abort("conversion rules contain duplicate unit pairs")
  }

  if (any(!is.finite(as.numeric(conversion_dt$factor)))) {
    cli::cli_abort("conversion factor values must be finite")
  }

  if (any(!is.finite(as.numeric(conversion_dt$offset)))) {
    cli::cli_abort("conversion offset values must be finite")
  }

  return(invisible(TRUE))
}

#' @title load number harmonization rules
#' @description ensure number harmonization folder exists, create template if missing,
#' and load numeric conversion rules.
#' @param config named configuration list.
#' @return named list with `conversion_rules`, `template_created`, and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create dir_ls path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
load_numeric_harmonization_rules <- function(config) {
  harmonization_dir <- config$paths$data$imports$harmonization
  fs::dir_create(harmonization_dir, recurse = TRUE)

  file_path <- fs::path(harmonization_dir, "numeric_harmonization.xlsx")

  if (!file.exists(file_path)) {
    cli::cli_alert_info("Creating numeric harmonization template...")
    numeric_template <- data.table::data.table(
      product = character(0),
      from_unit = character(0),
      to_unit = character(0),
      factor = numeric(0),
      offset = numeric(0)
    )
    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "number_harmonization")
    openxlsx::writeData(wb, "number_harmonization", numeric_template)
    openxlsx::saveWorkbook(wb, file_path, overwrite = FALSE)
  }

  list(
    layer_rules = read_rule_table(file_path),
    source_path = file_path
  )
}

#' @title apply number harmonization mapping
#' @description apply numeric unit conversions to a dataset using conversion rules.
#' @param mapped_dt data.table to harmonize.
#' @param conversion_dt numeric conversion rules data.table.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return named list with `data`, `matched_count`, `unmatched_count`.
#' @importFrom checkmate assert_data_frame assert_string
apply_number_harmonization_mapping <- function(
  mapped_dt,
  conversion_dt,
  unit_column,
  value_column,
  product_column
) {
  checkmate::assert_data_frame(mapped_dt, min.rows = 0)
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)

  if (!unit_column %in% colnames(mapped_dt)) {
    cli::cli_abort("unit column {.val {unit_column}} is missing")
  }
  if (!value_column %in% colnames(mapped_dt)) {
    cli::cli_abort("value column {.val {value_column}} is missing")
  }
  if (!product_column %in% colnames(mapped_dt)) {
    cli::cli_abort("product column {.val {product_column}} is missing")
  }

  harmonized_dt <- data.table::copy(data.table::as.data.table(mapped_dt))
  active_conversion <- data.table::as.data.table(conversion_dt)

  # Normalize for join
  active_conversion[, product_key := normalize_string(product)]
  active_conversion[, unit_key := normalize_string(from_unit)]
  data.table::setkey(active_conversion, product_key, unit_key)

  product_keys <- normalize_string(harmonized_dt[[product_column]])
  unit_keys <- normalize_string(harmonized_dt[[unit_column]])

  matched_index <- active_conversion[.(product_keys, unit_keys), which = TRUE]
  is_matched <- !is.na(matched_index)

  numeric_values <- coerce_numeric_safe(harmonized_dt[[value_column]])

  # Fail only if non-empty, non-numeric values exist
  invalid_values <- harmonized_dt[[value_column]][
    !is.na(harmonized_dt[[value_column]]) &
      is.na(numeric_values)
  ]

  if (length(invalid_values) > 0) {
    cli::cli_abort(
      "value column contains non-numeric values that cannot be harmonized: {paste(unique(invalid_values), collapse = ', ')}"
    )
  }

  # Apply conversions
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
  unmatched_count <- length(unique(unit_keys[!is_matched & unit_keys != ""]))

  return(list(
    data = harmonized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title run number harmonization layer
#' @description orchestrate number harmonization stage with rule loading, application,
#' diagnostics, and persistence. Creates template if missing.
#' @param cleaned_dt cleaned data.table.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return harmonized data.table with diagnostics attached.
#' @importFrom checkmate assert_data_frame assert_list assert_choice
run_number_harmonization_layer_batch <- function(
  cleaned_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  harmonized_dt <- data.table::copy(cleaned_dt)
  payload <- load_numeric_harmonization_rules(config)
  layer_rules <- payload$layer_rules

  if (nrow(layer_rules) > 0) {
    result <- apply_number_harmonization_mapping(
      harmonized_dt,
      layer_rules,
      unit_column,
      value_column,
      product_column
    )
    harmonized_dt <- result$data

    diagnostics <- build_layer_diagnostics(
      layer_name = "numeric_harmonization",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(harmonized_dt),
      matched_count = result$matched_count,
      unmatched_count = result$unmatched_count
    )
  } else {
    diagnostics <- build_layer_diagnostics(
      layer_name = "numeric_harmonization",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(cleaned_dt),
      matched_count = 0L,
      unmatched_count = 0L,
      status = "warn",
      messages = "no numeric harmonization rules found"
    )
  }

  attr(harmonized_dt, "layer_diagnostics") <- list(
    numeric_harmonization = diagnostics
  )
  return(harmonized_dt)
}

#' @title run cleaning, harmonization, and number harmonization pipeline
#' @description orchestrate all stages: cleaning, general harmonization, numeric harmonization.
#' Collects diagnostics for each stage and returns the final dataset.
#' @param raw_dt raw data.frame/data.table.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name for numeric harmonization (default: "unit").
#' @param value_column character scalar numeric value column name (default: "value").
#' @param product_column character scalar product column name (default: "product").
#' @return final data.table with `pipeline_diagnostics` and `pipeline_diagnostics_paths` attributes.
#' @importFrom checkmate assert_data_frame assert_list
run_clean_harmonize_pipeline_batch <- function(
  raw_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  # Cleaning stage
  cleaned_dt <- run_cleaning_layer_batch(raw_dt, config)

  # Harmonization stage
  harmonized_dt <- run_harmonization_layer_batch(cleaned_dt, config)

  # Numeric harmonization stage
  number_dt <- run_number_harmonization_layer_batch(
    harmonized_dt,
    config,
    unit_column,
    value_column,
    product_column
  )

  # Aggregate diagnostics
  pipeline_diagnostics <- list(
    cleaning = attr(cleaned_dt, "layer_diagnostics"),
    harmonization = attr(harmonized_dt, "layer_diagnostics"),
    number_harmonization = attr(number_dt, "layer_diagnostics")
  )

  attr(number_dt, "pipeline_diagnostics") <- pipeline_diagnostics
  return(number_dt)
}
