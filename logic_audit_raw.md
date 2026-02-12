## audit report

### naming and consistency findings

- `run_export_pipeline.R` and `31-export_data.R` still describe output as "final dataset" in comments, while runtime objects use `fao_data_raw`; this is semantic drift between docs and object lifecycle naming. locations:
  - `R/3-export_pipeline/run_export_pipeline.R:4,47`
  - `R/3-export_pipeline/31-export_data.R:3,8`
- transform-level naming now uses `wide_raw` / `long_raw`, but consolidation still exports as generic "processed"; the contract is valid but partially mixed across stage vocabulary (`raw` vs `processed`). locations:
  - `R/1-import_pipeline/12-transform.R:99,165-169`
  - `R/0-general_pipeline/01-setup.R:30-33,56-60`

### dry and fragmented logic findings

- script sourcing pattern is duplicated in three places (`run_pipeline.R`, `run_general_pipeline.R`, `run_import_pipeline.R`, `run_export_pipeline.R`), with identical walk/source behavior. this is repeated orchestration logic that can be centralized in one helper. locations:
  - `run_pipeline.R:7-9`
  - `R/0-general_pipeline/run_general_pipeline.R:10-18`
  - `R/1-import_pipeline/run_import_pipeline.R:9-20`
  - `R/3-export_pipeline/run_export_pipeline.R:11-19`
- progress handler setup is fragmented across import transform and export orchestration; both create progress handlers independently. locations:
  - `R/1-import_pipeline/12-transform.R:119-136`
  - `R/3-export_pipeline/run_export_pipeline.R:39-45`
- conversion boundaries are repeated (`as.data.table`, `rbindlist`, and per-step coercion) instead of enforcing one canonical data contract at stage entry/exit. locations:
  - `R/1-import_pipeline/11-reading.R:45,75,96`
  - `R/1-import_pipeline/12-transform.R:84,166-169`
  - `R/3-export_pipeline/run_export_pipeline.R:34`

### pipe usage findings

- no `%>%` usage was found. all observed pipeline chaining uses native `|>`. search command: `rg -n "%>%" R run_pipeline.R tests`.

### data type consistency findings

- import forces text read types (`col_types = "text"`) and later explicitly casts year columns to character, so year and value are consistently character in current pipeline; this is coherent but may defer numeric validation too late. locations:
  - `R/1-import_pipeline/11-reading.R:19`
  - `R/1-import_pipeline/12-transform.R:47-50`
- global option `stringsAsFactors = FALSE` is set, and no factor conversions are present in pipeline code. location:
  - `R/0-general_pipeline/01-setup.R:10-13`

## hardcoding proposals

| current dynamic logic | proposed hardcoded value | benefit |
|---|---|---|
| `enable_progress` parameter in `transform_files_list()` with branching to two code paths | remove parameter and always execute a single progress-enabled path | eliminates branching and duplicate loop logic for a stable operator-facing run mode |
| `config$export_config$lists_to_export` lookup at export runtime | hardcode a fixed vector in setup: `c("product", "variable", "unit", "continent", "country", "footnotes", "year", "notes", "yearbook", "document")` | removes repeated config dereference and prevents accidental runtime drift |
| dynamic suffix config (`data_suffix`, `list_suffix`) | hardcode as file-local constants in export helper | avoids indirect lookup for immutable naming convention |
| repeated `source()` vectors per stage | hardcode a single `source_stage_scripts(stage_name)` helper with fixed script maps | removes duplicated orchestration code and improves maintainability |
| runtime `exists("fao_data_raw")` gate in export orchestrator | hardcode function contract (`run_export_pipeline <- function(fao_data_raw, config)`) and call only through top-level runner | avoids global environment coupling and reduces hidden side effects |

## refactored snippets

### snippet 1: simplify import transform path with fixed progress behavior

```r
transform_files_list <- function(file_list_dt, read_data_list, config) {
  checkmate::assert_data_frame(file_list_dt)
  checkmate::assert_list(read_data_list)
  stopifnot(nrow(file_list_dt) == length(read_data_list))

  progressr::handlers(progressr::handler_txtprogressbar(clear = FALSE))

  results <- progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(nrow(file_list_dt)))

    purrr::map2(
      seq_len(nrow(file_list_dt)),
      read_data_list,
      function(index, df_wide) {
        progress(sprintf("processing file %d/%d", index, nrow(file_list_dt)))
        transform_single_file(file_list_dt[index, ], df_wide, config)
      }
    ) |>
      purrr::compact()
  })

  list(
    wide_raw = purrr::map(results, "wide_raw") |> data.table::rbindlist(fill = TRUE),
    long_raw = purrr::map(results, "long_raw") |> data.table::rbindlist(fill = TRUE)
  )
}
```

### snippet 2: hardcode stable export columns in setup

```r
load_pipeline_config <- function() {
  project_root <- here::here()
  build_path <- function(...) fs::path(project_root, ...)

  fixed_export_columns <- c(
    "product",
    "variable",
    "unit",
    "continent",
    "country",
    "footnotes",
    "year",
    "notes",
    "yearbook",
    "document"
  )

  export_config <- list(
    data_suffix = ".xlsx",
    list_suffix = "_unique.xlsx",
    lists_to_export = fixed_export_columns
  )

  # remaining config omitted for brevity
}
```

### snippet 3: remove duplicated source loops with one helper

```r
source_stage_scripts <- function(stage_name) {
  stage_scripts <- list(
    general = c("00-dependencies.R", "01-setup.R", "02-helpers.R"),
    import = c("10-file_io.R", "11-reading.R", "12-transform.R", "13-validate_log.R", "14-output.R"),
    export = c("31-export_data.R", "32-export_lists.R")
  )

  script_names <- stage_scripts[[stage_name]]

  purrr::walk(
    script_names,
    ~ source(here::here(paste0("R/", stage_name, "_pipeline"), .x), echo = FALSE)
  )
}
```

## trade-off summary

for this fixed-scope project, selective hardcoding improves startup predictability, removes branch-heavy orchestration, and reduces dynamic lookup overhead. the trade-off is lower portability across environments and less flexibility for ad-hoc datasets, but that trade-off is acceptable here because file naming, export schema, and run mode are operationally stable.
