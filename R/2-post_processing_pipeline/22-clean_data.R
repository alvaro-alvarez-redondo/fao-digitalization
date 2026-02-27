# script: cleaning stage functions
# description: load and apply text/data cleaning mappings and run the cleaning layer.


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

