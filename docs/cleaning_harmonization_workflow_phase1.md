# cleaning and harmonization workflow design - phase 1 proposal

## 1) conceptual foundations

### cleaning
cleaning standardizes raw labels and formats without changing analytical meaning.

scope:
- trim and normalize whitespace
- case normalization
- encoding normalization
- deterministic replacement of placeholder tokens for missing values
- footnote/artifact stripping only when defined by mapping rules

non-scope:
- taxonomy reassignment
- unit conversion
- aggregation
- category collapsing

contract:
- row count invariant
- key structure invariant
- idempotent outputs
- deterministic one-to-one mapping per target column

### harmonization
harmonization aligns cleaned records to analytical standards.

scope:
- taxonomy/code mapping
- numeric unit harmonization via conversion factors
- canonical key generation for downstream uniqueness

contract:
- controlled structural changes are allowed
- unmapped required keys are fail-fast or explicitly reported
- deterministic conversion and classification

## 2) layered workflow structure

execution order:
1. raw dataset (`fao_data_raw`)
2. cleaning layer (`run_cleaning_layer()`)
3. cleaned dataset (`fao_data_cleaned`)
4. harmonization layer (`run_harmonization_layer()`)
5. optional aggregation (`aggregate_harmonized_data(aggregation = TRUE)`)
6. final dataset (`fao_data_final`)

boundary guarantees:
- each layer receives and returns explicit contracts
- each layer writes auditable artifacts (matched/unmatched summaries)
- each layer is independently re-runnable

## 3) diagnostics standardization (cross-layer contract)

all layers must emit a standardized diagnostics object to simplify audit exports and automated validation.

required diagnostics fields:
- `layer_name` (`"cleaning" | "harmonization" | "aggregation"`)
- `execution_timestamp_utc` (iso-8601 string)
- `rule_version_cleaning` (character, nullable outside cleaning)
- `rule_version_taxonomy` (character, nullable outside harmonization)
- `rule_version_conversion` (character, nullable outside harmonization)
- `rows_in` (integer)
- `rows_out` (integer)
- `matched_count` (integer)
- `unmatched_count` (integer)
- `idempotence_passed` (logical)
- `validation_passed` (logical)
- `status` (`"pass" | "warn" | "fail"`)
- `messages` (character vector)

diagnostics persistence:
- write one json/csv artifact per layer under `data/audit/<dataset>/`
- aggregate diagnostics in a run-level summary for final reporting

## 4) cleaning layer design

### 4.1 functional surface (proposed)
- `load_cleaning_rules(config)`
- `validate_cleaning_rules(rules_dt, target_columns)`
- `apply_cleaning_rules(dataset_dt, rules_dt, key_columns = character())`
- `run_cleaning_layer(dataset_dt, config)`

### 4.2 mapping file model
source location (already scaffolded by config):
- `config$paths$data$imports$cleaning`

required columns in mapping workbook/table:
- `target_column` (character)
- `original_value` (character)
- `cleaned_value` (character)
- `rule_version` (character/date-like)
- `active_flag` (logical/integer)

constraints:
- uniqueness: (`target_column`, `original_value`, `rule_version`, `active_flag == TRUE`)
- deterministic: one input maps to one output
- no empty `target_column` or `original_value`

### 4.3 schema validation before transformation
schema validation must run before any row-level transformation.

required checks:
- mandatory columns exist
- column types match expected classes
- `active_flag` coercible to logical
- `rule_version` non-empty for active rows
- no duplicated active keys

on schema failure:
- abort run with controlled error (`cli::cli_abort()`)

### 4.4 algorithm
for each configured `target_column`:
1. normalize rule keys using existing normalization helpers (lower/ascii/squish policy).
2. normalize input column to a temporary standardized key.
3. left join input to rules by (`target_column`, normalized original key).
4. replace value with `cleaned_value` when match exists; otherwise preserve original.
5. record unmatched distinct values per column.

output objects:
- cleaned dataset (same schema as input)
- standardized diagnostics object (section 3)

### 4.5 cleaning validations
mandatory checks:
- `rows_in == rows_out`
- no duplicate active rules on (`target_column`, `original_value`)
- no `NA` in required rule fields
- second pass over cleaned data is identical to first pass

failure mode:
- hard fail via `cli::cli_abort()` for rule integrity violations
- unmatched values are controlled by config policy:
  - `cleaning_unmatched_policy = "report" | "fail"`

idempotency marker:
- set `_cleaned_flag` (logical true) and `_cleaning_rule_version` in output metadata/columns.
- if `_cleaned_flag` already true with same rule version, skip reapplication and emit warning diagnostics.

## 5) harmonization layer design

### 5.1 functional surface (proposed)
- `load_harmonization_rules(config)`
- `validate_taxonomy_rules(taxonomy_dt)`
- `validate_conversion_rules(conversion_dt)`
- `apply_taxonomy_mapping(cleaned_dt, taxonomy_dt)`
- `apply_numeric_harmonization(mapped_dt, conversion_dt)`
- `run_harmonization_layer(cleaned_dt, config)`

### 5.2 taxonomy mapping model
source location:
- `config$paths$data$imports$harmonization`

required taxonomy fields:
- `entity_key` (input from cleaned data)
- `canonical_entity`
- `taxonomy_code`
- `hierarchy_level`
- `valid_from`, `valid_to` (optional temporal control)
- `active_flag`

constraints:
- one active mapping per input key at execution date
- many-to-one allowed (multiple keys -> one canonical entity)
- required output fields must be non-missing when active

### 5.3 numeric harmonization model
required conversion fields:
- `from_unit`
- `to_unit`
- `factor`
- `offset` (default 0)
- `active_flag`

deterministic formula:
- `value_harmonized = value * factor + offset`

constraints:
- positive/non-zero factor where applicable
- only one active conversion path for a (`from_unit`, `to_unit`) pair
- conversion marker column prevents repeated application

### 5.4 harmonization validations
- fail/report unmatched taxonomy keys
- fail on ambiguous conversion rules
- assert numeric column type before conversion
- assert no repeated conversion when marker indicates already harmonized
- run schema checks for taxonomy and conversion rule tables before mapping

idempotency marker:
- set `_harmonized_flag` and `_harmonization_rule_version`.
- if `_harmonized_flag` already true with same version, skip conversion/mapping and emit warning diagnostics.

## 6) aggregation layer design

### 6.1 activation rule
aggregation executes only when explicit argument is set:
- `aggregation = TRUE`

### 6.2 grouping specification
grouping keys must be provided in config, for example:
- `config$harmonization$aggregation_keys`

numeric measure fields explicitly listed:
- `config$harmonization$aggregation_value_columns`

### 6.3 deterministic aggregation
- group by configured analytical unit keys
- sum value columns with `na.rm = TRUE`
- preserve non-aggregated descriptor fields only when functionally dependent or re-derived

post-aggregation safety checks:
- assert uniqueness on analytical unit keys
- assert no required grouping key is missing
- assert numeric overflow/underflow checks on aggregated measures
- assert dependent descriptor columns did not lose deterministic relationship

post-condition:
- one row per analytical unit key
- uniqueness assertion must pass

## 7) mapping governance principles

external-control rules:
- no embedded literal mapping logic in transformation code
- all mappings loaded from governed files
- every mapping file versioned and timestamped
- schema validation before execution

governance checks:
- duplicate key detection
- orphan required columns detection
- active version conflict detection
- unmatched key reporting artifact generation

## 8) integration plan with current repository

existing repository anchors:
- path scaffolding for cleaning/harmonization imports already exists in config creation.
- current pipeline order is general -> import -> export.

phase-1 integration proposal:
1. add new stage scripts under `R/2-clean_harmonize_pipeline/`:
   - `20-cleaning.R`
   - `21-harmonization.R`
   - `22-aggregation.R`
   - `run_clean_harmonize_pipeline.R`
2. wire `run_pipeline()` sequence to include stage 2 between import and export.
3. persist diagnostics to `data/audit/<dataset>/` alongside current audit outputs.

## 9) extensibility pattern

add new target columns/entity types without core code edits by:
- extending mapping tables with additional `target_column`/`entity_key` entries
- updating config-driven column lists only
- preserving generic join/apply functions
- validating new schema rows through existing validators

compatibility rule:
- new mapping content must not require branching logic in transformation functions.

## 10) system guarantees (if implemented as specified)

- semantic consistency: cleaning standardizes representation only.
- structural coherence: harmonization aligns categories/codes deterministically.
- numerical comparability: conversion logic is explicit and reproducible.
- deterministic reproducibility: rule-driven, versioned mappings with validations.
- extensibility: new rule tables can extend behavior without changing core logic.

## 11) phase-1 implementation boundaries

in-scope for next implementation step:
- rule schema validators
- cleaning and harmonization executors
- diagnostics artifacts and fail/report policies
- deterministic aggregation gate

out-of-scope for this proposal:
- domain-specific taxonomy content
- production mapping files
- performance tuning beyond deterministic vectorized joins
