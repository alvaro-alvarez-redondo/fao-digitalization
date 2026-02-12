## 2. Structural / Architectural Improvements

- **Move from script-sourcing orchestration to function-based orchestration.** Current top-level execution relies on `source()` side effects and global objects (`config`, `fao_data_raw`) instead of explicit return values. This limits composability, observability, and testability. Introduce `run_general_pipeline()`, `run_import_pipeline(config)`, and `run_export_pipeline(data, config)` as pure entry points and reserve scripts for thin CLI wrappers.
- **Introduce explicit pipeline contracts between stages.** `run_import_pipeline.R` currently creates multiple globals (`fao_data_wide`, `fao_data_long`, `collected_errors`, etc.). Replace this with structured return objects (`list(data = ..., diagnostics = ...)`) to enforce data contracts and reduce accidental coupling.
- **Unify data backend strategy.** The code mixes tidyverse and `data.table` heavily (often converting back-and-forth). Standardize either on tidyverse tibbles with dplyr verbs or on data.table end-to-end per stage; if mixed, define clear boundaries to avoid copy overhead and style drift.
- **Add a configuration layer external to code.** Paths and export behavior are embedded in `01-setup.R`. Promote YAML/JSON-driven config and environment-specific overrides for production portability.
- **Package-ize the project structure.** Move reusable functions into `R/` package-style functions with roxygen docs and exported internals. This will improve namespace discipline and unlock proper `testthat` discovery.

## 3. Code-Level Improvements

- **Fix error handling in dependency loading.** `purrr::walk(..., error = ...)` is invalid for `walk()`. Use `purrr::walk(packages, purrr::possibly(...))` or `tryCatch` around `library()` calls.
- **Eliminate implicit global dependencies.** `read_excel_sheet()` reads `config` from the parent environment instead of accepting it as an argument. This creates hidden coupling and non-deterministic behavior in tests.
- **Fix import error collection bug.** `read_data_list` maps only the `$data` element, then `collected_reading_errors` incorrectly attempts to extract `$errors` from data tables; this silently drops sheet/file reading errors.
- **Avoid viewer side effects in pipelines.** `View(fao_data_raw)` in `run_pipeline.R` breaks non-interactive execution and CI; guard it behind interactive checks or remove.
- **Replace remaining base pipe assignments and formatting drift.** Native pipe usage is mostly consistent, but indentation in `01-setup.R` (`export_config`) is inconsistent and weakens readability.
- **Normalize language in comments/messages.** One comment remains Spanish (`TRUE si NA o ""`). Standardize all comments and diagnostics to professional English.
- **Harden column-level assertions.** `get_unique_column()` does not assert the requested column exists before indexing. Add explicit checks to fail fast with clear messages.
- **Reduce duplication in helper utilities.** `ensure_data_table()` and `as_data_table_safe()` duplicate behavior; keep one canonical helper.

## 4. Performance Observations

- **Repeated conversions likely create avoidable copies.** Multiple functions convert to/from data.table/tibble repeatedly. For large FAO-like datasets this will increase memory churn.
- **Row-wise split + map for validation is expensive.** `split(fao_data_long, document)` materializes many subsets; consider grouped validation using data.table by-groups to reduce allocations.
- **Use keyed/ indexed duplicate detection for scale.** Duplicate checks can benefit from keyed data.table columns (`setkeyv`) before `by=` aggregations for very large inputs.
- **Excel export can become bottleneck.** Exporting one workbook per column list is I/O-heavy. Consider batching lists into one workbook with multiple tabs in high-volume runs.

## 5. Quick Wins (Low-effort, high-impact fixes)

1. Remove or guard `View()` with `if (interactive())`.
2. Pass `config` explicitly into `read_excel_sheet()` and dependent calls.
3. Correct error collection in import stage to retain both `data` and `errors` from `read_file_sheets()`.
4. Replace invalid `walk(..., error=)` pattern with safe loader logic.
5. Merge `ensure_data_table()` and `as_data_table_safe()` into one helper.
6. Translate Spanish comments to English and run a consistent formatter (e.g., styler).
7. Add basic tests for `identify_year_columns()`, `discover_files()`, and `validate_long_dt()`.

## 6. Long-Term Recommendations

- **Adopt a formal workflow framework** (`targets` or `drake`) for dependency-aware, reproducible, incremental runs.
- **Add observability and resilience:** structured logging (e.g., `logger`), explicit warning/error registries, and optional fail-fast vs fail-soft modes.
- **Strengthen dependency management:** pin versions with `renv.lock`, run CI checks for lockfile drift, and remove unused packages (`progress` appears redundant when `progressr` is used).
- **Build production test layers:** unit tests for helpers, integration tests with representative Excel fixtures, and regression tests for schema changes.
- **Define data contracts explicitly:** schema validation (e.g., `pointblank`/`validate`) for each stage boundary.
- **Document operating model:** README runbook including expected folder layout, config parameters, failure modes, and recovery steps.
