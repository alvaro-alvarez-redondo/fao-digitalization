# script: cleaning stage functions
# description: load and apply text/data cleaning mappings and run the cleaning layer.

#' @title load cleaning rules
#' @description discover and load cleaning mapping files from the configured
#' cleaning-import directory. when no files are found, create a template file and
#' return it as the only source.
#' @param config named configuration list.
#' @return named list where each element contains `layer_rules` and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_ls dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom purrr map
#' @examples
#' \dontrun{load_cleaning_rules(config)}
load_cleaning_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$cleaning, min.chars = 1)

  cleaning_dir <- config$paths$data$imports$cleaning
  fs::dir_create(cleaning_dir, recurse = TRUE)

  files <- fs::dir_ls(
    cleaning_dir,
    regexp = "^cleaning_.*\\.xlsx$",
    type = "file"
  )

  if (length(files) == 0) {
    template_path <- fs::path(cleaning_dir, "cleaning_template.xlsx")

    if (!file.exists(template_path)) {
      cli::cli_alert_info("No cleaning files found, creating template...")

      cleaning_template <- data.table::data.table(
        column_source = character(0),
        column_target = character(0),
        original_value_source = character(0),
        original_value_target = character(0),
        cleaned_value_target = character(0)
      )

      workbook <- openxlsx::createWorkbook()
      openxlsx::addWorksheet(workbook, "cleaning_mapping")
      openxlsx::writeData(workbook, "cleaning_mapping", cleaning_template)
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

#' @title apply cleaning rules
#' @description apply deterministic one-to-one cleaning mappings for each
#' source-target column pair using vectorized normalized-key joins.
#' @param dataset_dt input data.table or data.frame.
#' @param layer_rules cleaning rules data.table or data.frame.
#' @param key_columns optional key columns preserved for context.
#' @return named list with cleaned data, matched count, and unmatched count.
#' @importFrom checkmate assert_data_frame assert_character assert_names
#' @importFrom purrr reduce
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
  checkmate::assert_names(
    names(layer_rules),
    must.include = c(
      "column_source",
      "column_target",
      "original_value_source",
      "original_value_target",
      "cleaned_value_target"
    )
  )

  mapped_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  active_rules <- data.table::as.data.table(layer_rules)
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
          cleaned_value_target
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
        current_dt[
          is_matched,
          (tgt_col) := join_result$cleaned_value_target[is_matched]
        ]
      }

      matched_delta <- as.integer(sum(is_matched))
      unmatched_delta <- as.integer(sum(
        !is_matched & !is.na(current_dt[[src_col]])
      ))

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

#' @title run cleaning layer batch
#' @description execute rule-driven cleaning with schema checks, deterministic
#' diagnostics collection, and idempotent mapping application.
#' @param dataset_dt input data.frame/data.table.
#' @param config named configuration list.
#' @return cleaned data.table with diagnostics in attributes.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom purrr reduce
#' @examples
#' \dontrun{run_cleaning_layer_batch(fao_data_raw, config)}
run_cleaning_layer_batch <- function(dataset_dt, config) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  initial_dt <- data.table::copy(data.table::as.data.table(dataset_dt))
  layer_payloads <- load_cleaning_rules(config)

  cleaning_state <- purrr::reduce(
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
        value_column = "cleaned_value_target",
        layer_name = basename(source_path)
      )

      result <- apply_cleaning_mapping(state$data, layer_rules)

      diagnostics <- build_layer_diagnostics(
        layer_name = basename(source_path),
        rows_in = nrow(dataset_dt),
        rows_out = nrow(result$data),
        matched_count = result$matched_count,
        unmatched_count = result$unmatched_count
      )

      state$diagnostics[[basename(source_path)]] <- diagnostics
      state$data <- result$data

      return(state)
    }
  )

  cleaned_dt <- cleaning_state$data
  attr(cleaned_dt, "layer_diagnostics") <- cleaning_state$diagnostics

  return(cleaned_dt)
}
