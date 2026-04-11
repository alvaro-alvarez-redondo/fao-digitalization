# General Project Performance

- Analysis timestamp (UTC): 2026-04-07T12:06:32Z
- Preset: standard
- Overall pipeline class: O(n^3)
- Runtime bottleneck pipeline: perf_1-import_pipeline
- Complexity bottleneck pipeline: perf_1-import_pipeline
- Primary function bottleneck: normalize_for_comparison (3-export)

## Pipeline Summary

```text
| Pipeline                        | Total functions | Highest-complexity function        | Dominant complexity | Identified bottlenecks                           | Runtime share |
| ------------------------------- | --------------- | ---------------------------------- | ------------------- | ------------------------------------------------ | ------------- |
| perf_0-general_pipeline         | 3               | drop_na_value_rows (O(n^2))        | O(n^2)              | drop_na_value_rows (O(n^2), score 0.3632)        | 1.5%          |
| perf_1-import_pipeline          | 7               | discover_excel_files (O(n^3))      | O(n^3)              | read_excel_file_sheets (O(1), score 0.4077)      | 87.9%         |
| perf_2-post_processing_pipeline | 3               | aggregate_standardized_rows (O(n)) | O(n)                | aggregate_standardized_rows (O(n), score 0.3359) | 8.4%          |
| perf_3-export_pipeline          | 2               | normalize_for_comparison (O(n))    | O(n)                | normalize_for_comparison (O(n), score 0.4671)    | 2.2%          |
```

## Cross-Stage Runtime and Risk Ranking

```text
| Pipeline                        | Runtime share | Stage risk score | Dominant complexity | Top runtime driver                  | Top composite bottleneck             | Low-confidence share | High-volatility share | Critical bottlenecks |
| ------------------------------- | ------------- | ---------------- | ------------------- | ----------------------------------- | ------------------------------------ | -------------------- | --------------------- | -------------------- |
| perf_1-import_pipeline          | 87.9%         | 0.6372           | O(n^3)              | read_excel_file_sheets (73.6%)      | read_excel_file_sheets (0.4077)      | 14.3%                | 14.3%                 | 0                    |
| perf_2-post_processing_pipeline | 8.4%          | 0.2004           | O(n)                | aggregate_standardized_rows (61.7%) | aggregate_standardized_rows (0.3359) | 0.0%                 | 66.7%                 | 0                    |
| perf_3-export_pipeline          | 2.2%          | 0.1587           | O(n)                | normalize_for_comparison (99.2%)    | normalize_for_comparison (0.4671)    | 0.0%                 | 50.0%                 | 0                    |
| perf_0-general_pipeline         | 1.5%          | 0.2395           | O(n^2)              | coerce_numeric_safe (38.3%)         | drop_na_value_rows (0.3632)          | 0.0%                 | 0.0%                  | 1                    |
```

## Stage Bottleneck Matrix

```text
| Pipeline                        | Runtime driver                      | Asymptotic driver                  | Composite-score driver               | Priority | Stage risk score | Low-confidence fns | High-volatility fns |
| ------------------------------- | ----------------------------------- | ---------------------------------- | ------------------------------------ | -------- | ---------------- | ------------------ | ------------------- |
| perf_0-general_pipeline         | coerce_numeric_safe (38.3%)         | drop_na_value_rows (O(n^2))        | drop_na_value_rows (0.3632)          | P0       | 0.2395           | 0                  | 0                   |
| perf_1-import_pipeline          | read_excel_file_sheets (73.6%)      | discover_excel_files (O(n^3))      | read_excel_file_sheets (0.4077)      | P2       | 0.6372           | 1                  | 1                   |
| perf_2-post_processing_pipeline | aggregate_standardized_rows (61.7%) | aggregate_standardized_rows (O(n)) | aggregate_standardized_rows (0.3359) | P2       | 0.2004           | 0                  | 2                   |
| perf_3-export_pipeline          | normalize_for_comparison (99.2%)    | normalize_for_comparison (O(n))    | normalize_for_comparison (0.4671)    | P2       | 0.1587           | 0                  | 1                   |
```

## Global Top Bottleneck Functions

```text
| Rank | Function                    | Pipeline                        | Complexity | Composite score | Stage impact | Confidence | Volatility cv | Priority | Flags                                           | Likely slowdown drivers                              |
| ---- | --------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------- | ------------- | -------- | ----------------------------------------------- | ---------------------------------------------------- |
| 1    | normalize_for_comparison    | perf_3-export_pipeline          | O(n)       | 0.4671          | 99.2%        | very high  | 0.10912       | P2       | high_impact                                     | sorting/reordering work adds comparison overhead; t~ |
| 2    | read_excel_file_sheets      | perf_1-import_pipeline          | O(1)       | 0.4077          | 73.6%        | very low   | 0.067524      | P2       | high_impact/low_confidence                      | runtime concentration is high (73.6% of stage runti~ |
| 3    | drop_na_value_rows          | perf_0-general_pipeline         | O(n^2)     | 0.3632          | 35.2%        | very high  | 0.076319      | P0       | high_complexity/high_impact/critical_bottleneck | super-linear class (O(n^2)) raises asymptotic risk;~ |
| 4    | aggregate_standardized_rows | perf_2-post_processing_pipeline | O(n)       | 0.3359          | 61.7%        | very high  | 0.11549       | P2       | high_impact                                     | grouping across repeated keys increases hash/scan c~ |
| 5    | normalize_string_impl       | perf_0-general_pipeline         | O(n)       | 0.3276          | 26.5%        | very high  | 0.029774      | P2       | ok                                              | string normalization/coercion is CPU and allocation~ |
| 6    | reshape_to_long             | perf_1-import_pipeline          | O(n^2)     | 0.3171          | 22.0%        | very high  | 0.050175      | P2       | high_complexity                                 | reshape pressure likely amplifies memory movement; ~ |
| 7    | discover_excel_files        | perf_1-import_pipeline          | O(n^3)     | 0.3051          | 0.3%         | high       | 0.11205       | P2       | high_complexity                                 | super-linear class (O(n^3)) raises asymptotic risk   |
| 8    | coerce_numeric_safe         | perf_0-general_pipeline         | O(n)       | 0.2542          | 38.3%        | very high  | 0.040136      | P2       | high_impact                                     | string normalization/coercion is CPU and allocation~ |
| 9    | apply_standardize_rules     | perf_2-post_processing_pipeline | O(n)       | 0.2098          | 25.2%        | very high  | 0.33129       | P2       | high_volatility                                 | grouping across repeated keys increases hash/scan c~ |
| 10   | extract_aggregated_rows     | perf_2-post_processing_pipeline | O(n)       | 0.1659          | 13.1%        | very high  | 0.40185       | P2       | high_volatility                                 | grouping across repeated keys increases hash/scan c~ |
| 11   | normalize_key_fields        | perf_1-import_pipeline          | O(n)       | 0.1283          | 2.2%         | very high  | 0.39262       | P2       | high_volatility                                 | reshape pressure likely amplifies memory movement; ~ |
| 12   | detect_duplicates_dt        | perf_1-import_pipeline          | O(n)       | 0.1251          | 1.4%         | very high  | 0.03108       | P2       | ok                                              | grouping across repeated keys increases hash/scan c~ |
```

## Recommended Optimization Roadmap

### Quick Wins

```text
| Function                    | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                              | Expected impact           |
| --------------------------- | ------------------------------- | ---------- | --------------- | ------------ | --------------------------------------------------- | ------------------------- |
| normalize_for_comparison    | perf_3-export_pipeline          | O(n)       | 0.4671          | 99.2%        | large runtime share; sharp growth jump (max 13.09x) | up to 99.2% stage runtime |
| read_excel_file_sheets      | perf_1-import_pipeline          | O(1)       | 0.4077          | 73.6%        | large runtime share; low model confidence           | up to 73.6% stage runtime |
| aggregate_standardized_rows | perf_2-post_processing_pipeline | O(n)       | 0.3359          | 61.7%        | large runtime share; sharp growth jump (max 10.98x) | up to 61.7% stage runtime |
| coerce_numeric_safe         | perf_0-general_pipeline         | O(n)       | 0.2542          | 38.3%        | large runtime share; sharp growth jump (max 12.58x) | up to 38.3% stage runtime |
| normalize_string_impl       | perf_0-general_pipeline         | O(n)       | 0.3276          | 26.5%        | sharp growth jump (max 9.87x)                       | up to 26.5% stage runtime |
```

### Medium Effort

```text
| Function                | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| ----------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| read_excel_file_sheets  | perf_1-import_pipeline          | O(1)       | 0.4077          | 73.6%        | large runtime share; low model confidence            | up to 73.6% stage runtime |
| reshape_to_long         | perf_1-import_pipeline          | O(n^2)     | 0.3171          | 22.0%        | super-linear growth risk; sharp growth jump (max 32~ | up to 22.0% stage runtime |
| discover_excel_files    | perf_1-import_pipeline          | O(n^3)     | 0.3051          | 0.3%         | super-linear growth risk                             | up to 0.3% stage runtime  |
| apply_standardize_rules | perf_2-post_processing_pipeline | O(n)       | 0.2098          | 25.2%        | unstable repeated timings; sharp growth jump (max 2~ | up to 25.2% stage runtime |
| extract_aggregated_rows | perf_2-post_processing_pipeline | O(n)       | 0.1659          | 13.1%        | unstable repeated timings; sharp growth jump (max 1~ | up to 13.1% stage runtime |
```

### High Effort / High Impact

```text
| Function           | Pipeline                | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| ------------------ | ----------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| drop_na_value_rows | perf_0-general_pipeline | O(n^2)     | 0.3632          | 35.2%        | large runtime share; super-linear growth risk; shar~ | up to 35.2% stage runtime |
```

## Project Narrative

- Runtime is most concentrated in perf_1-import_pipeline (87.9% of observed cross-stage runtime).
- Asymptotic risk is highest in perf_1-import_pipeline (dominant class O(n^3)).
- Optimize first: normalize_for_comparison in perf_3-export_pipeline (score 0.4671, impact 99.2%).
