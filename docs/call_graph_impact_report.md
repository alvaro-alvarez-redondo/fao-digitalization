# call graph impact analyzer report

## 1. dependency graph summary

Top-level orchestration call path now executes explicit runner functions after sourcing stage scripts:

- `run_pipeline()`
  - sources: `run_general_pipeline.R`, `run_import_pipeline.R`, `run_export_pipeline.R`
  - calls: `run_general_pipeline()` -> `run_import_pipeline(config)` -> `run_export_pipeline(fao_data_raw, config)`

Import-stage graph (high-level):

- `run_import_pipeline()`
  - `source_import_scripts()`
  - `discover_files()`
  - `read_pipeline_files()` -> `map_with_progress()` -> `safe_execute_read()` -> `read_file_sheets()` -> `read_excel_sheet()`
  - `transform_files_list()`
  - `validate_long_dt()`
  - `consolidate_audited_dt()`

Export-stage graph (high-level):

- `run_export_pipeline()`
  - `source_export_scripts()`
  - `ensure_data_table()`
  - `audit_data_output()`
  - `export_processed_data()`
  - `export_selected_unique_lists()`

## 2. high-impact functions

1. `run_pipeline()`
   - root orchestrator for all stage runners; breakage propagates to all integration flows.
2. `run_import_pipeline()`
   - central bridge between file discovery, read path, transform path, and validation/output contracts.
3. `read_pipeline_files()` and `read_file_sheets()`
   - fan-out over all files/sheets; contract or error-shape changes ripple into import diagnostics and downstream auditing.
4. `consolidate_audited_dt()`
   - final import-stage schema gate; any output-shape/type change directly affects export-stage assumptions.
5. `run_export_pipeline()`
   - final publishing stage; depends on stabilized import schema and config layout.

## 3. change ripple risk assessment

- **signature ripple risk:** low.
  - No runner function signatures changed in this iteration.
- **behavior ripple risk:** medium.
  - `run_pipeline()` behavior is now explicit execution of stage runners after sourcing scripts, reducing hidden side effects but changing execution from source-only orchestration.
- **downstream breakage risk:** low-medium.
  - Existing contracts for return types are preserved (`invisible(TRUE)` from `run_pipeline`; structured import/export lists maintained).
- **cross-file coupling risk:** medium.
  - Coupling remains via config schema and shared canonical columns across import/export modules.
- **cyclic dependency risk:** low.
  - Static call path remains acyclic: general -> import -> export with utility helpers; no direct mutual recursion detected.

## 4. stability map

| area | status | notes |
|---|---|---|
| runner signatures/defaults | stable | no signature/default changes for orchestrator/import/export runners. |
| runner output structures | stable | preserved top-level return shapes; diagnostics and schema outputs remain explicit. |
| orchestration determinism | improved | explicit runner execution in `run_pipeline()` removes source-only ambiguity. |
| schema boundary stability | improved | consolidation continues enforcing canonical order and typed outputs. |
| legacy compatibility surface | managed | legacy wrappers remain available for migration paths that rely on global assignment side effects. |

## 5. implementations completed

- Updated `run_pipeline()` to execute stage runners explicitly after sourcing and to fail fast when required runner functions are missing.
- Added/updated tests to validate stage execution ordering and missing-runner abort behavior.
- Preserved existing signatures and return contracts while reducing call-graph ambiguity at the orchestration root.
