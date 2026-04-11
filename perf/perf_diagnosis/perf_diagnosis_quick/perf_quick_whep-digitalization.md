# General Project Performance

- Analysis timestamp (UTC): 2026-04-11T09:21:56Z
- Preset: quick
- Overall pipeline class: unknown
- Runtime bottleneck pipeline: perf_1-import_pipeline
- Complexity bottleneck pipeline: perf_0-general_pipeline
- Primary function bottleneck: normalize_string_impl (0-general)

## Pipeline Summary

```text
| Pipeline                        | Total functions | Highest-complexity function           | Dominant complexity | Identified bottlenecks                              | Runtime share |
| ------------------------------- | --------------- | ------------------------------------- | ------------------- | --------------------------------------------------- | ------------- |
| perf_0-general_pipeline         | 3               | normalize_string_impl (unknown)       | unknown             | normalize_string_impl (unknown, score 0.8083)       | 0.0%          |
| perf_1-import_pipeline          | 7               | read_excel_file_sheets (unknown)      | unknown             | read_excel_file_sheets (unknown, score 0.7988)      | 99.9%         |
| perf_2-post_processing_pipeline | 3               | aggregate_standardized_rows (unknown) | unknown             | aggregate_standardized_rows (unknown, score 0.6326) | 0.1%          |
| perf_3-export_pipeline          | 2               | normalize_for_comparison (unknown)    | unknown             | normalize_for_comparison (unknown, score 0.7703)    | 0.0%          |
```

## Cross-Stage Runtime and Risk Ranking

```text
| Pipeline                        | Runtime share | Stage risk score | Dominant complexity | Top runtime driver                  | Top composite bottleneck             | Low-confidence share | High-volatility share | Critical bottlenecks |
| ------------------------------- | ------------- | ---------------- | ------------------- | ----------------------------------- | ------------------------------------ | -------------------- | --------------------- | -------------------- |
| perf_1-import_pipeline          | 99.9%         | 0.8282           | unknown             | read_excel_file_sheets (99.7%)      | read_excel_file_sheets (0.7988)      | 100.0%               | 14.3%                 | 1                    |
| perf_2-post_processing_pipeline | 0.1%          | 0.4669           | unknown             | aggregate_standardized_rows (52.2%) | aggregate_standardized_rows (0.6326) | 100.0%               | 33.3%                 | 1                    |
| perf_3-export_pipeline          | 0.0%          | 0.5              | unknown             | normalize_for_comparison (91.5%)    | normalize_for_comparison (0.7703)    | 100.0%               | 50.0%                 | 1                    |
| perf_0-general_pipeline         | 0.0%          | 0.4334           | unknown             | normalize_string_impl (63.6%)       | normalize_string_impl (0.8083)       | 100.0%               | 0.0%                  | 1                    |
```

## Stage Bottleneck Matrix

```text
| Pipeline                        | Runtime driver                      | Asymptotic driver                     | Composite-score driver               | Priority | Stage risk score | Low-confidence fns | High-volatility fns |
| ------------------------------- | ----------------------------------- | ------------------------------------- | ------------------------------------ | -------- | ---------------- | ------------------ | ------------------- |
| perf_0-general_pipeline         | normalize_string_impl (63.6%)       | normalize_string_impl (unknown)       | normalize_string_impl (0.8083)       | P0       | 0.4334           | 3                  | 0                   |
| perf_1-import_pipeline          | read_excel_file_sheets (99.7%)      | read_excel_file_sheets (unknown)      | read_excel_file_sheets (0.7988)      | P0       | 0.8282           | 7                  | 1                   |
| perf_2-post_processing_pipeline | aggregate_standardized_rows (52.2%) | aggregate_standardized_rows (unknown) | aggregate_standardized_rows (0.6326) | P0       | 0.4669           | 3                  | 1                   |
| perf_3-export_pipeline          | normalize_for_comparison (91.5%)    | normalize_for_comparison (unknown)    | normalize_for_comparison (0.7703)    | P0       | 0.5              | 2                  | 1                   |
```

## Global Top Bottleneck Functions

```text
| Rank | Function                     | Pipeline                        | Complexity | Composite score | Stage impact | Confidence | Volatility cv | Priority | Flags                                                | Likely slowdown drivers                              |
| ---- | ---------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------- | ------------- | -------- | ---------------------------------------------------- | ---------------------------------------------------- |
| 1    | normalize_string_impl        | perf_0-general_pipeline         | unknown    | 0.8083          | 63.6%        | unknown    | 0.071403      | P0       | high_complexity/high_impact/low_confidence/critical~ | string normalization/coercion is CPU and allocation~ |
| 2    | read_excel_file_sheets       | perf_1-import_pipeline          | unknown    | 0.7988          | 99.7%        | unknown    | 0.027033      | P0       | high_complexity/high_impact/low_confidence/critical~ | super-linear class (unknown) raises asymptotic risk~ |
| 3    | normalize_for_comparison     | perf_3-export_pipeline          | unknown    | 0.7703          | 91.5%        | unknown    | 0.081379      | P0       | high_complexity/high_impact/low_confidence/critical~ | sorting/reordering work adds comparison overhead; t~ |
| 4    | aggregate_standardized_rows  | perf_2-post_processing_pipeline | unknown    | 0.6326          | 52.2%        | unknown    | 0.32695       | P0       | high_complexity/high_impact/low_confidence/high_vol~ | grouping across repeated keys increases hash/scan c~ |
| 5    | extract_aggregated_rows      | perf_2-post_processing_pipeline | unknown    | 0.5618          | 31.9%        | unknown    | 0.086017      | P1       | high_complexity/low_confidence                       | grouping across repeated keys increases hash/scan c~ |
| 6    | coerce_numeric_safe          | perf_0-general_pipeline         | unknown    | 0.5252          | 21.5%        | unknown    | 0.18309       | P2       | high_complexity/low_confidence                       | string normalization/coercion is CPU and allocation~ |
| 7    | apply_standardize_rules      | perf_2-post_processing_pipeline | unknown    | 0.5056          | 15.9%        | unknown    | 0.096412      | P2       | high_complexity/low_confidence                       | grouping across repeated keys increases hash/scan c~ |
| 8    | drop_na_value_rows           | perf_0-general_pipeline         | unknown    | 0.5022          | 14.9%        | unknown    | 0.19737       | P2       | high_complexity/low_confidence                       | super-linear class (unknown) raises asymptotic risk~ |
| 9    | compute_unique_column_values | perf_3-export_pipeline          | unknown    | 0.4797          | 8.5%         | unknown    | 0.2741        | P2       | high_complexity/low_confidence/high_volatility       | sorting/reordering work adds comparison overhead; s~ |
| 10   | discover_excel_files         | perf_1-import_pipeline          | unknown    | 0.4511          | 0.3%         | unknown    | 0.077297      | P2       | high_complexity/low_confidence                       | super-linear class (unknown) raises asymptotic risk~ |
| 11   | normalize_key_fields         | perf_1-import_pipeline          | unknown    | 0.45            | 0.0%         | unknown    | 0.11817       | P2       | high_complexity/low_confidence                       | reshape pressure likely amplifies memory movement; ~ |
| 12   | detect_duplicates_dt         | perf_1-import_pipeline          | unknown    | 0.45            | 0.0%         | unknown    | 0.16434       | P2       | high_complexity/low_confidence                       | grouping across repeated keys increases hash/scan c~ |
```

## Recommended Optimization Roadmap

### Quick Wins

```text
| Function | Pipeline | Complexity | Composite score | Stage impact | Reason                   | Expected impact |
| -------- | -------- | ---------- | --------------- | ------------ | ------------------------ | --------------- |
| N/A      | N/A      | unknown    | N/A             | 0.0%         | No candidates identified | N/A             |
```

### Medium Effort

```text
| Function                     | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| ---------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| extract_aggregated_rows      | perf_2-post_processing_pipeline | unknown    | 0.5618          | 31.9%        | super-linear growth risk; low model confidence       | up to 31.9% stage runtime |
| coerce_numeric_safe          | perf_0-general_pipeline         | unknown    | 0.5252          | 21.5%        | super-linear growth risk; low model confidence; sha~ | up to 21.5% stage runtime |
| apply_standardize_rules      | perf_2-post_processing_pipeline | unknown    | 0.5056          | 15.9%        | super-linear growth risk; low model confidence; sha~ | up to 15.9% stage runtime |
| drop_na_value_rows           | perf_0-general_pipeline         | unknown    | 0.5022          | 14.9%        | super-linear growth risk; low model confidence; sha~ | up to 14.9% stage runtime |
| compute_unique_column_values | perf_3-export_pipeline          | unknown    | 0.4797          | 8.5%         | super-linear growth risk; unstable repeated timings~ | up to 8.5% stage runtime  |
```

### High Effort / High Impact

```text
| Function                    | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| --------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| normalize_string_impl       | perf_0-general_pipeline         | unknown    | 0.8083          | 63.6%        | large runtime share; super-linear growth risk; low ~ | up to 63.6% stage runtime |
| read_excel_file_sheets      | perf_1-import_pipeline          | unknown    | 0.7988          | 99.7%        | large runtime share; super-linear growth risk; low ~ | up to 99.7% stage runtime |
| normalize_for_comparison    | perf_3-export_pipeline          | unknown    | 0.7703          | 91.5%        | large runtime share; super-linear growth risk; low ~ | up to 91.5% stage runtime |
| aggregate_standardized_rows | perf_2-post_processing_pipeline | unknown    | 0.6326          | 52.2%        | large runtime share; super-linear growth risk; unst~ | up to 52.2% stage runtime |
```

## Project Narrative

- Runtime is most concentrated in perf_1-import_pipeline (99.9% of observed cross-stage runtime).
- Asymptotic risk is highest in perf_1-import_pipeline (dominant class unknown).
- Optimize first: normalize_string_impl in perf_0-general_pipeline (score 0.8083, impact 63.6%).
