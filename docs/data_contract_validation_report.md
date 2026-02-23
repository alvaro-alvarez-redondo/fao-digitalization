# data contract validation report

## 1. input contract map

| function | documented inputs | enforced checks | contract notes |
|---|---|---|---|
| `run_import_pipeline(config)` | `config$paths$data$imports$raw` required | `assert_list`, `assert_string`, `assert_directory_exists` | assumes file discovery returns `document` + `file_path` schema via downstream file io contract. |
| `read_excel_sheet(file_path, sheet_name, config)` | file path, sheet name, `config$column_required` | `check_string`, `check_list`, `check_character` | missing required columns are created as `NA_character_`, making base schema resilient. |
| `read_file_sheets(file_path, config)` | file path + config | same family of checks | sheet names are treated as implicit metadata and surfaced as warning strings for non-ascii names. |
| `read_pipeline_files(file_list_dt, config)` | table with `file_path`; config with `column_required` | `check_data_frame`, `check_names`, `check_character`, `check_list` | stable empty contract returned for zero-row input. |
| `consolidate_audited_dt(dt_list, config)` | list-like data items; `config$column_order` | `check_list` + `validate_output_column_order` | now guarantees schema columns even when no data items are provided. |

## 2. output contract map

| function | output structure | stability status | implementation update |
|---|---|---|---|
| `read_excel_sheet()` | `list(data = data.table, errors = character)` | stable | retained; ensures `variable` and required base columns exist before return. |
| `read_file_sheets()` | `list(data = data.table, errors = character)` | stable | switched to fixed-list error accumulation and single bind to reduce transient growth while preserving shape. |
| `read_pipeline_files()` | `list(read_data_list = list[data.table], errors = character)` | stable | removed `transpose()` path; now deterministic one-pass accumulation with same structure. |
| `consolidate_audited_dt()` | `list(data = data.table, warnings = character)` | improved | empty consolidations now return typed empty table with configured `column_order` columns. |
| `run_import_pipeline()` | `list(data, wide_raw, diagnostics)` | improved | diagnostics fields are now normalized to explicit character vectors even when downstream returns `NULL`. |

## 3. schema risk assessment

- **undocumented input schemas (medium):** some cross-script assumptions are implicit (for example `discover_files()` output columns consumed by import runner).
- **unstable output structures (reduced to low):** empty consolidation previously returned an untyped/no-column table; now stabilized with configured schema columns.
- **inconsistent column typing (medium):** schema columns can arrive with mixed types from heterogeneous sources; consolidation now normalizes configured schema columns to character.
- **implicit schema assumptions (medium-high):** downstream stages still rely on expected canonical columns such as `document`, `year`, and `value`.
- **column order instability (low):** `consolidate_audited_dt()` enforces `column_order` first and preserves extras after the canonical schema.

## 4. contract stabilization plan

1. add explicit reusable contract helpers (input/output schema assertions) for runner boundaries.
2. keep canonical schema and type normalization at consolidation boundaries.
3. add dedicated tests for empty-result schemas and diagnostics type guarantees.
4. document inter-stage column contracts in one source-of-truth schema specification.
5. add CI checks in an R-enabled environment to execute full testthat suite for contract regressions.

## 5. implementations completed

- refactored `run_import_pipeline()` to normalize diagnostics outputs to character vectors.
- refactored `read_file_sheets()` and `read_pipeline_files()` accumulation logic to maintain contracts with lower temporary object growth.
- updated `consolidate_audited_dt()` to return a schema-stable empty table and normalize canonical schema columns to character.
- added deterministic tests for empty consolidation schema stability and diagnostics type normalization.
