# script: general harmonization stage functions
# description: load and apply categorical harmonization mappings and run the harmonization layer.

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
    template_path <- fs::path(harmonization_dir, "harmonization_template.xlsx")

    if (!file.exists(template_path)) {
      cli::cli_alert_info("No harmonization files found, creating template...")

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
    }

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
