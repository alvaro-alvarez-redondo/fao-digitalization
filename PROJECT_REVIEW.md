## 1. Executive summary

the repository has a strong foundational structure with clear stage separation (`0-general`, `1-import`, `3-export`) and meaningful functional decomposition inside each stage. however, production readiness is currently limited by script-level side effects, heavy reliance on global objects (`config`, `fao_data_raw`), and mixed backend patterns (`tidyverse` + `data.table`) that increase cognitive load and maintenance cost.

the pipeline is close to a maintainable baseline, but it still behaves like an exploratory workflow rather than a contract-driven production system. key risks are orchestration coupling through `source()`, weak stage interfaces, partial validation ergonomics, and missing observability patterns for long-running jobs.

---

## 2. structural / architectural improvements

- **replace side-effect orchestration with explicit stage contracts.**
  current execution chains rely on sourcing scripts that mutate the global environment. migrate to explicit APIs:
  - `run_general_pipeline() -> config`
  - `run_import_pipeline(config) -> list(data, diagnostics)`
  - `run_export_pipeline(data, config) -> list(paths, diagnostics)`
  this will eliminate hidden dependencies and make integration testing straightforward.

- **move from script-first to package-style organization.**
  keep entry scripts thin, and move reusable logic into package-like function files with roxygen docs and controlled namespace usage. this removes duplicate sourcing logic and supports scalable testing.

- **introduce a diagnostics object across the full pipeline.**
  instead of creating multiple globals (`collected_reading_errors`, `collected_errors`, `collected_warnings`), use one structured object:
  - `diagnostics$errors$reading`
  - `diagnostics$errors$validation`
  - `diagnostics$warnings$consolidation`
  this improves observability and downstream reporting.

- **externalize runtime configuration.**
  move path and export settings to yaml/json + environment overrides. this is required for reproducible multi-environment deployments.

- **define and enforce stage schemas.**
  formalize required columns and expected types at boundaries. validate on entry/exit of every stage to catch drift early.

---

## 3. code-level improvements

- **standardize backend strategy.**
  the project frequently converts objects between tibble and `data.table`. either:
  - use tidyverse end-to-end, or
  - isolate `data.table` to specific performance-critical steps with explicit boundaries.
  current mixed style increases copy risk and review complexity.

- **remove hidden state assumptions.**
  several scripts assume `config` and `fao_data_raw` already exist. pass these as function arguments and return values only.

- **harden validation messages.**
  mandatory-field checks currently generate repeated per-document messages without row-level context. include row ids, column names, and severity for actionable debugging.

- **improve duplicate detection ergonomics.**
  duplicate checks are correct but return concatenated strings only. return a structured table of duplicate keys and counts, then format human-readable logs as a separate concern.

- **finish english standardization in comments/documentation.**
  there is still spanish text (`bloques semánticos`) in setup comments. replace with professional english consistently across all scripts.

- **reduce duplication in assertion and conversion helpers.**
  keep one canonical coercion helper and one validation helper per concern to preserve single responsibility.

- **maintain strict lowercase and snake_case conventions consistently.**
  current naming is mostly compliant; preserve this rule in all new functions, parameters, and generated fields.

---

## 4. performance observations

- **avoid repeated materialization from `split()` + `map()` during validation.**
  splitting by document allocates many intermediate objects. perform grouped validation by keyed `data.table` operations to reduce memory churn.

- **limit conversion churn between data structures.**
  repeated coercions (`as.data.table`, tibble pipelines, melts, rebinds) can significantly increase allocation overhead on large excel batches.

- **optimize duplicate detection for scale.**
  set keys or indexes on duplicate-check columns before grouped counts in high-volume datasets.

- **review export strategy under high output cardinality.**
  creating one workbook per unique-list column is manageable now, but may become i/o bound at scale. consider batched workbooks with one sheet per list.

---

## 5. quick wins

1. convert script-level execution to function returns in `run_import_pipeline` and `run_export_pipeline` while keeping current behavior via thin wrappers.
2. introduce a single `pipeline_result` object that contains `data` plus all diagnostics.
3. translate remaining non-english comments to professional english.
4. add unit tests for:
   - `identify_year_columns()`
   - `discover_files()`
   - `validate_long_dt()`
   - `consolidate_validated_dt()`
5. replace ad-hoc message strings with structured log records (stage, level, code, message, context).
6. run `styler` and `lintr` in ci to enforce style consistency.

---

## 6. long-term recommendations

- **adopt `targets` for orchestration and reproducibility.**
  this gives deterministic dependency graphs, caching, parallelism, and robust reruns.

- **add full dependency pinning with `renv.lock` governance.**
  require lockfile updates in pull requests and enforce reproducibility checks in ci.

- **implement layered testing strategy.**
  - unit tests for helpers and validators
  - integration tests with realistic excel fixtures
  - regression tests for schema drift and duplicate logic

- **introduce production logging and failure policy controls.**
  add configurable `fail_fast` vs `fail_soft`, structured logs, and summary artifacts for monitoring.

- **document operational runbook.**
  include input conventions, expected file naming patterns, schema contracts, common failures, and recovery playbooks for long-term maintainability.
