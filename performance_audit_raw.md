## bottleneck map

1. `run_import_pipeline.R` keeps full duplicated intermediates (`read_results`, `read_data_list`, `transformed`, `fao_data_wide_raw`, `fao_data_long_raw`, `validation_results`, `validated_dt_list`) before consolidation.
   - why slow: large list/data.table objects coexist, increasing peak memory and gc overhead.
   - locations: `R/1-import_pipeline/run_import_pipeline.R:34-67`.

2. `run_import_pipeline.R` validates by `split()` on document and then `map()`.
   - why slow: `split()` materializes many copies for large document counts.
   - locations: `R/1-import_pipeline/run_import_pipeline.R:55-56`.

3. `12-transform.R` has dual execution paths (`process_files_with_progress`, `process_files_no_progress`) plus final `map()` extraction + `rbindlist()`.
   - why slow: duplicate orchestration logic and extra list traversal at scale.
   - locations: `R/1-import_pipeline/12-transform.R:119-170`.

4. `11-reading.R` reads each sheet with `col_types = "text"` and then transforms repeatedly.
   - why slow: fully character loads increase object size for numeric year columns and trigger later conversion passes.
   - locations: `R/1-import_pipeline/11-reading.R:15-20`, `R/1-import_pipeline/12-transform.R:47-50`.

5. `convert_year_columns()` copies full tables before rename (`copy(df)` + `setnames`).
   - why slow: full-table copy is expensive for wide data and can double memory footprint temporarily.
   - location: `R/1-import_pipeline/12-transform.R:41-42`.

6. export stage writes one workbook per list column.
   - why slow: repeated workbook construction and disk i/o dominates runtime when many list columns are exported.
   - locations: `R/3-export_pipeline/32-export_lists.R:21-44`, `R/3-export_pipeline/run_export_pipeline.R:57-59`.

7. repeated `data.table` coercion/checks in read, transform, validate, and export.
   - why slow: repeated class checks/coercions add overhead and hide contract boundaries.
   - locations: `R/1-import_pipeline/11-reading.R:45`, `R/1-import_pipeline/13-validate_log.R:64`, `R/3-export_pipeline/run_export_pipeline.R:34`.

## optimization proposals

### 1) avoid `split()` validation copies

before:

```r
validation_results <- split(fao_data_long_raw, fao_data_long_raw$document) |>
  purrr::map(~ validate_long_dt(.x, config))
```

after:

```r
validation_results <- fao_data_long_raw[, .(data = list(.sd)), by = document] |>
  (
    \(grouped_dt) {
      purrr::map(grouped_dt$data, ~ validate_long_dt(.x, config))
    }
  )()
```

expected gain: reduced object duplication versus base `split()` and better locality when document cardinality is high.

### 2) stream transform + bind in one pass

before:

```r
results <- purrr::map2(...)

list(
  wide_raw = purrr::map(results, "wide_raw") |> data.table::rbindlist(fill = fill_missing),
  long_raw = purrr::map(results, "long_raw") |> data.table::rbindlist(fill = fill_missing)
)
```

after:

```r
results <- purrr::map2(...)

wide_raw <- data.table::rbindlist(lapply(results, `[[`, "wide_raw"), fill = fill_missing)
long_raw <- data.table::rbindlist(lapply(results, `[[`, "long_raw"), fill = fill_missing)

list(wide_raw = wide_raw, long_raw = long_raw)
```

expected gain: fewer `purrr::map()` traversals and slightly lower overhead in tight loops.

### 3) avoid full copy during year-column rename

before:

```r
clean_names <- gsub("\\.0$", "", colnames(df))
df <- data.table::setnames(copy(df), old = colnames(df), new = clean_names)
```

after:

```r
clean_names <- gsub("\\.0$", "", colnames(df))
if (!identical(clean_names, colnames(df))) {
  data.table::setnames(df, old = colnames(df), new = clean_names)
}
```

expected gain: eliminates full-table copy when rename can be done by reference.

### 4) reduce repeated excel write setup for unique lists

before:

```r
purrr::walk(cols_to_export, function(col_name) {
  export_single_column_list(fao_data_raw, col_name, config, overwrite)
  p()
})
```

after:

```r
export_lists_batch <- function(df, cols_to_export, config, overwrite, fill_missing) {
  wb <- openxlsx::createWorkbook()

  purrr::walk(cols_to_export, function(col_name) {
    values <- get_unique_column(df, col_name)
    openxlsx::addWorksheet(wb, col_name)
    openxlsx::writeData(wb, col_name, values)
  })

  path <- generate_export_path(config, "fao_unique_lists_raw", type = "lists")
  openxlsx::saveWorkbook(wb, path, overwrite = overwrite)
  path
}
```

expected gain: significantly less i/o and workbook creation overhead.

### 5) enforce a single data contract at import boundary

before:

```r
read_data_list <- purrr::map(read_results, "data")
transformed <- transform_files_list(...)
fao_data_wide_raw <- transformed$wide_raw
fao_data_long_raw <- transformed$long_raw
```

after:

```r
read_data_list <- purrr::map(read_results, "data")
rm(read_results)

transformed <- transform_files_list(...)
fao_data_long_raw <- transformed$long_raw
rm(transformed, read_data_list)
```

expected gain: lower peak memory during mid-pipeline phases.

## memory management tips

- explicitly `rm()` heavy intermediates after each stage and call `gc()` after consolidation and after exports.
- prefer by-reference operations in `data.table` (`setnames`, `:=`) to avoid full-table copies.
- precompute `cols_to_export <- config$export_config$lists_to_export` once per run and pass explicitly to avoid repeated lookups.
- avoid storing both wide and long artifacts if only long is needed downstream; gate wide output behind a flag.
- keep `col_types` explicit for known stable columns (for example, fixed text metadata + numeric year columns when parsing is stable) to reduce parse ambiguity and reduce conversion passes.

## tidyverse-first guidance

- keep `|>` chaining and dplyr-style readability for orchestration-level code.
- only use deeper `data.table` by-reference optimization in the highest-cost hotspots (`convert_year_columns`, grouped validation, large binds).
- avoid switching engines broadly; isolate optimizations to hot functions to retain maintainability.
