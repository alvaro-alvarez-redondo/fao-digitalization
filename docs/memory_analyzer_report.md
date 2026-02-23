# memory analyzer report

## 1. memory risk map

| area | risk | severity | evidence |
|---|---|---|---|
| import runner validation grouping | explicit full-table copies per document group can multiply resident memory for large `long_raw` tables | high | `run_import_pipeline()` builds `validation_groups` with `data.table::copy(.SD)` inside grouped list creation. |
| audit export preparation | redundant copy before coercion retains an avoidable duplicate object | high | `export_audit_report_to_excel()` creates `export_dt <- data.table::as.data.table(data.table::copy(audit_dt))`. |
| read pipeline aggregation | list-of-results and repeated flattening keep multiple intermediate lists alive (`read_results`, transposed lists, per-file lists) | medium | `read_pipeline_files()` stores `read_results`, then `parsed_results`, then `per_file_results` prior to final assembly. |
| sheet-level aggregation | all sheet data tables are retained in `sheets_list` before a final `rbindlist`, increasing peak memory on multi-sheet workbooks | medium | `read_file_sheets()` maps all sheets into `sheets_list` and only then binds results. |
| legacy wrappers/global state | global assignments prolong object lifetime and can prevent garbage collection of large pipeline artifacts in long sessions | medium | legacy wrappers assign `import_pipeline_result`, `fao_data_raw`, `fao_data_wide_raw`, and `export_paths` into `.GlobalEnv`. |
| closure/environment capture | helper wrappers and progress closures are short-lived; no long-lived hidden closures detected in main pipeline paths | low | no persistent closure storage pattern found from static scan. |

## 2. high-allocation hotspots

1. `R/1-import_pipeline/run_import_pipeline.R`:
   - grouped validation staging copies each document subset (`data.table::copy(.SD)`) before validation.
   - risk profile: peak-memory growth with document count and row volume.

2. `R/3-export_pipeline/30-data_audit.R`:
   - export staging uses double conversion/copy (`as.data.table(copy(...))`) when `audit_dt` is already tabular.
   - risk profile: large duplicate allocation just before workbook write.

3. `R/1-import_pipeline/11-reading.R` (`read_file_sheets`, `read_pipeline_files`):
   - full list materialization for sheets/files and multiple post-processing passes (`transpose`, multiple `map("...")` + `unlist`).
   - risk profile: elevated temporary allocations and delayed release of intermediates.

4. `R/1-import_pipeline/15-output.R` (`consolidate_audited_dt`):
   - consolidates complete `dt_items` list then performs `rbindlist`; expected but can spike memory when many large items are present.
   - risk profile: unavoidable combine allocation; candidate for chunked consolidation if datasets grow further.

## 3. optimization candidates

- reduce per-group copying in import validation by validating grouped views directly when mutation isolation is not required.
- replace `as.data.table(copy(audit_dt))` with minimal conversion path and copy only when subsequent in-place mutation would alter caller-owned data.
- in sheet/file readers, prefer streaming-style accumulation (append/bind in chunks) to reduce simultaneous retention of all per-sheet/per-file tables.
- collapse intermediate list passes in `read_pipeline_files()` (single-pass extraction of `data` and `errors`) to reduce transient object growth.
- keep legacy wrappers for compatibility, but document that global-assignment paths are memory-heavier and should be avoided in production batch loops.
- for very large workloads, add optional explicit cleanup points (`rm(...)` on large intermediates after bind) in long functions once compatibility constraints allow.
