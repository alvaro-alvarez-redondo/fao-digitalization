# script: 92-synthetic_data
# description: synthetic data generators for the Big O complexity analysis
#   module. all functions produce self-contained in-memory objects and perform
#   no I/O. safe to call from any benchmark closure.
#
# sourced by: run_complexity_analysis.R

# ── 2. synthetic data generators ────────────────────────────────────────────

#' @title sample product labels
.products     <- c("cereals", "oilseeds", "pulses", "fruits", "vegetables",
                   "sugar", "roots", "cotton", "tobacco", "fibres")
#' @title sample variable labels
.variables    <- c("production", "yield", "area_harvested", "import_quantity",
                   "export_quantity", "feed", "seed", "stock_variation")
#' @title sample unit labels
.units        <- c("tonnes", "kg_ha", "ha", "usd", "1000_usd", "head")
#' @title sample continent labels
.continents   <- c("asia", "europe", "africa", "americas", "oceania")
#' @title sample country pool
.countries    <- paste0("country_", formatC(1:80, width = 2, flag = "0"))

#' @title make synthetic wide-format data table
#' @description produces a data.table in the wide pipeline format with `n` rows
#'   and `n_years` year columns. safe for all import-stage benchmarks.
#' @param n integer. number of observation rows.
#' @param n_years integer. number of year columns (default 10).
#' @return data.table with key columns and year columns named "2000".."200x".
make_wide_dt <- function(n, n_years = 10L) {
  year_cols <- as.character(seq(2000L, 2000L + n_years - 1L))
  dt <- data.table::data.table(
    product    = sample(.products,   n, replace = TRUE),
    variable   = sample(.variables,  n, replace = TRUE),
    unit       = sample(.units,      n, replace = TRUE),
    continent  = sample(.continents, n, replace = TRUE),
    country    = sample(.countries,  n, replace = TRUE),
    footnotes  = sample(c(NA_character_, "e", "f", "p"), n,
                        replace = TRUE, prob = c(0.7, 0.1, 0.1, 0.1))
  )
  year_vals <- matrix(
    ifelse(
      stats::runif(n * n_years) < 0.9,
      as.character(round(stats::runif(n * n_years, 0, 1e6), 1)),
      NA_character_
    ),
    nrow = n,
    ncol = n_years
  )
  for (i in seq_along(year_cols)) {
    data.table::set(dt, j = year_cols[[i]], value = year_vals[, i])
  }
  return(dt)
}

#' @title make synthetic long-format data table
#' @description produces a data.table in the long pipeline format used by
#'   post-import stages. all columns match the expected schema.
#' @param n integer. number of observation rows.
#' @param na_fraction numeric in [0,1]. fraction of rows with NA value.
#' @param dup_fraction numeric in [0,1]. fraction of rows that are exact
#'   duplicates of another row (for duplicate-detection benchmarks).
#' @return data.table with full long-format schema.
make_long_dt <- function(n, na_fraction = 0.0, dup_fraction = 0.0) {
  n_dup   <- as.integer(floor(n * dup_fraction))
  n_orig  <- n - n_dup

  dt <- data.table::data.table(
    product   = sample(.products,   n_orig, replace = TRUE),
    variable  = sample(.variables,  n_orig, replace = TRUE),
    unit      = sample(.units,      n_orig, replace = TRUE),
    continent = sample(.continents, n_orig, replace = TRUE),
    country   = sample(.countries,  n_orig, replace = TRUE),
    year      = sample(as.character(1990L:2020L), n_orig, replace = TRUE),
    value     = ifelse(
      stats::runif(n_orig) < na_fraction,
      NA_character_,
      as.character(round(stats::runif(n_orig, 0, 1e6), 2))
    ),
    notes     = NA_character_,
    yearbook  = sample(c("yearbook_2020", "yearbook_2021"), n_orig, replace = TRUE),
    document  = sample(paste0("file_", formatC(1:5, width = 2, flag = "0"), ".xlsx"),
                       n_orig, replace = TRUE),
    footnotes = sample(c(NA_character_, "e", "f", "p"), n_orig,
                       replace = TRUE, prob = c(0.7, 0.1, 0.1, 0.1))
  )

  if (n_dup > 0L) {
    dup_idx <- sample(seq_len(n_orig), n_dup, replace = TRUE)
    dt      <- data.table::rbindlist(list(dt, dt[dup_idx]))
  }

  return(dt)
}

#' @title make synthetic numeric-string vector
#' @description creates a character vector suitable for `coerce_numeric_safe()`.
#' @param n integer. vector length.
#' @return character vector with mix of numeric strings, empty strings, and NAs.
make_numeric_string_vec <- function(n) {
  pool <- c(
    as.character(round(stats::runif(max(n, 100L), -1e6, 1e6), 4)),
    rep("", 5L),
    rep(NA_character_, 5L)
  )
  sample(pool, n, replace = TRUE)
}

#' @title make a minimal pipeline config for benchmarking
#' @description returns a minimal config list containing only the fields
#'   required by the benchmarked functions. does NOT create directories or
#'   perform any I/O.
#' @return named list (subset of full pipeline config).
make_benchmark_config <- function() {
  col_order <- c(
    "hemisphere", "continent", "country", "product", "variable",
    "unit", "year", "value", "notes", "yearbook", "document", "footnotes"
  )
  list(
    column_required = c("product", "variable", "unit", "continent", "country"),
    column_id       = c("product", "variable", "unit", "hemisphere",
                        "continent", "country", "footnotes"),
    column_order    = col_order,
    defaults        = list(notes_value = NA_character_),
    columns = list(
      mandatory = c("product", "variable", "unit", "value"),
      base      = c("continent", "country", "unit", "footnotes"),
      id        = c("product", "variable", "unit", "hemisphere",
                    "continent", "country", "footnotes"),
      value     = c("year", "value")
    )
  )
}
