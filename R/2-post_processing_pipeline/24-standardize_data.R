# script: general harmonization stage functions
# description: load and apply categorical harmonization mappings and run the harmonization layer.

#' @title load harmonization rules
#' @description load value-renaming harmonization mappings from the harmonization
#' imports folder. when no mappings are found, create and return a template.
#' @param config named configuration list.
#' @return named list with per-file payloads containing `layer_rules` and
#' `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom purrr map
#' @examples
#' \dontrun{load_harmonization_rules(config)}
load_harmonization_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$harmonization, min.chars = 1)

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

      workbook <- openxlsx::createWorkbook()
      openxlsx::addWorksheet(workbook, "harmonization_mapping")
      openxlsx::writeData(workbook, "harmonization_mapping", harmonization_template)
      openxlsx::saveWorkbook(workbook, template_path, overwrite = FALSE)
    }

    files <- template_path
  }

  layer_payloads <- purrr::map(files, \(file_path) {
    return(list(
      layer_rules = read_rule_table(file_path),
      source_path = file_path
    ))
  })

  return(layer_payloads)
}

#' @title validate harmonization rule uniqueness
#' @description ensure many-to-one deterministic harmonization by preventing
#' duplicate source-target key mappings per column pair.
#' @param harmonization_dt harmonization rules data.table/data.frame.
#' @return invisible `TRUE`.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{validate_harmonization_rule_uniqueness(harmonization_dt)}
validate_harmonization_rule_uniqueness <- function(harmonization_dt) {
  checkmate::assert_data_frame(harmonization_dt, min.rows = 1)

  rules_dt <- data.table::as.data.table(harmonization_dt)
  duplicate_rules <- rules_dt[
    ,
    .N,
    by = .(
      column_source,
      column_target,
      original_value_source,
      original_value_target
    )
  ][N > 1]

  if (nrow(duplicate_rules) > 0) {
    cli::cli_abort(
      "harmonization rules contain duplicate mappings for the same source-target key"
    )
  }

  return(invisible(TRUE))
}

#' @title apply harmonization mapping
#' @description apply deterministic many-to-one taxonomy harmonization using
#' vectorized source-target key joins.
#' @param cleaned_dt cleaned data.table/data.frame.
#' @param harmonization_dt harmonization rules data.table/data.frame.
#' @return named list with harmonized data, matched count, unmatched count.
#' @importFrom checkmate assert_data_frame assert_names
#' @importFrom purrr reduce
#' @examples
#' \dontrun{apply_harmonization_mapping(cleaned_dt, harmonization_dt)}
apply_harmonization_mapping <- function(cleaned_dt, harmonization_dt) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonization_dt, min.rows = 1)
  checkmate::assert_names(
    names(harmonization_dt),
    must.include = c(
      "column_source",
      "column_target",
      "original_value_source",
      "original_value_target",
      "harmonized_value_target"
    )
  )

  validate_harmonization_rule_uniqueness(harmonization_dt)

  mapped_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  active_rules <- data.table::as.data.table(harmonization_dt)
  pair_plan <- unique(active_rules[, .(column_source, column_target)])

  initial_state <- list(
    data = mapped_dt,
    matched_count = 0L,
    unmatched_count = 0L
  )

  final_state <- purrr::reduce(
    .x = seq_len(nrow(pair_plan)),
    .init = initial_state,
    .f = function(state, pair_row_index) {
      src_col <- pair_plan$column_source[[pair_row_index]]
      tgt_col <- pair_plan$column_target[[pair_row_index]]
      current_dt <- state$data

      if (!src_col %in% colnames(current_dt)) {
        cli::cli_warn(
          "column_source {.val {src_col}} not found in dataset, skipping"
        )

        return(state)
      }

      if (!tgt_col %in% colnames(current_dt)) {
        cli::cli_warn(
          "column_target {.val {tgt_col}} not found; creating NA column"
        )
        current_dt[, (tgt_col) := NA_character_]
      }

      column_rules <- active_rules[
        column_source == src_col & column_target == tgt_col,
        .(
          source_key = normalize_string(original_value_source),
          target_key = normalize_string(original_value_target),
          harmonized_value_target
        )
      ]

      join_input <- data.table::data.table(
        row_id = seq_len(nrow(current_dt)),
        source_key = normalize_string(current_dt[[src_col]]),
        target_key = normalize_string(current_dt[[tgt_col]])
      )

      join_result <- column_rules[
        join_input,
        on = .(source_key, target_key)
      ]

      is_matched <- !is.na(join_result$source_key)

      if (any(is_matched)) {
        current_dt[is_matched, (tgt_col) := join_result$harmonized_value_target[is_matched]]
      }

      matched_delta <- as.integer(sum(is_matched))
      unmatched_delta <- as.integer(sum(!is_matched & !is.na(current_dt[[src_col]])))

      return(list(
        data = current_dt,
        matched_count = state$matched_count + matched_delta,
        unmatched_count = state$unmatched_count + unmatched_delta
      ))
    }
  )

  return(list(
    data = final_state$data,
    matched_count = as.integer(final_state$matched_count),
    unmatched_count = as.integer(final_state$unmatched_count)
  ))
}


#' @title run harmonization layer batch
#' @description execute value-renaming harmonization with schema checks,
#' deterministic diagnostics, and vectorized mapping application.
#' @param cleaned_dt cleaned data.frame/data.table.
#' @param config named configuration list.
#' @return harmonized data.table with diagnostics attributes.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom purrr reduce
#' @examples
#' \dontrun{run_harmonization_layer_batch(cleaned_dt, config)}
run_harmonization_layer_batch <- function(cleaned_dt, config) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  initial_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  layer_payloads <- load_harmonization_rules(config)

  harmonization_state <- purrr::reduce(
    layer_payloads,
    .init = list(data = initial_dt, diagnostics = list()),
    .f = function(state, payload) {
      layer_rules <- payload$layer_rules
      source_path <- payload$source_path

      if (nrow(layer_rules) == 0) {
        return(state)
      }

      validate_mapping_rules(
        layer_rules,
        dataset_columns = colnames(state$data),
        value_column = "harmonized_value_target",
        layer_name = basename(source_path)
      )

      result <- apply_harmonization_mapping(state$data, layer_rules)

      diagnostics <- build_layer_diagnostics(
        layer_name = basename(source_path),
        rows_in = nrow(cleaned_dt),
        rows_out = nrow(result$data),
        matched_count = result$matched_count,
        unmatched_count = result$unmatched_count
      )

      state$diagnostics[[basename(source_path)]] <- diagnostics
      state$data <- result$data

      return(state)
    }
  )

  harmonized_dt <- harmonization_state$data
  attr(harmonized_dt, "layer_diagnostics") <- harmonization_state$diagnostics

  return(harmonized_dt)
}
