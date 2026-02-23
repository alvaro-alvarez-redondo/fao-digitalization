# benchmark: base-column row filtering in read_excel_sheet
# scope: compare original vs optimized keep_row calculation only

build_sample_dt <- function(n_rows = 200000L, n_cols = 6L) {
  dt <- data.table::data.table(
    country = sample(c("argentina", "", NA_character_), n_rows, replace = TRUE),
    continent = sample(c("americas", "", NA_character_), n_rows, replace = TRUE),
    unit = sample(c("t", "", NA_character_), n_rows, replace = TRUE),
    footnotes = sample(c("none", "", NA_character_), n_rows, replace = TRUE)
  )

  extra_cols <- max(0L, n_cols - 4L)
  if (extra_cols > 0L) {
    for (index in seq_len(extra_cols)) {
      dt[[paste0("extra_", index)]] <- sample(c("x", "", NA_character_), n_rows, replace = TRUE)
    }
  }

  return(dt)
}

original_keep_row <- function(read_dt, base_cols) {
  keep_row <- read_dt[,
    rowSums(!is.na(as.matrix(.SD)) & trimws(as.matrix(.SD)) != "") > 0,
    .SDcols = base_cols
  ]

  return(keep_row)
}

optimized_keep_row <- function(read_dt, base_cols) {
  base_matrix <- as.matrix(read_dt[, ..base_cols])
  keep_row <- rowSums(!is.na(base_matrix) & nzchar(trimws(base_matrix))) > 0

  return(keep_row)
}

sample_dt <- build_sample_dt()
base_columns <- c("country", "continent", "unit", "footnotes")

benchmark_results <- bench::mark(
  original = original_keep_row(sample_dt, base_columns),
  optimized = optimized_keep_row(sample_dt, base_columns),
  iterations = 30,
  check = TRUE
)

print(benchmark_results)
