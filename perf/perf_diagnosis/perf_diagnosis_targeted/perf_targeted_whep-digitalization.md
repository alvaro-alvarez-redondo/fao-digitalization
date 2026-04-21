# General Project Performance

- Analysis timestamp (UTC): 2026-04-11T09:59:09Z
- Preset: targeted
- Overall pipeline class: O(n log n)
- Runtime bottleneck pipeline: perf_2-post_processing_pipeline
- Complexity bottleneck pipeline: perf_0-general_pipeline
- Primary function bottleneck: normalize_for_comparison (3-export)

## Pipeline Summary

```text
| Pipeline                        | Total functions | Highest-complexity function        | Dominant complexity | Identified bottlenecks                           | Runtime share |
| ------------------------------- | --------------- | ---------------------------------- | ------------------- | ------------------------------------------------ | ------------- |
| perf_0-general_pipeline         | 3               | drop_na_value_rows (O(n log n))    | O(n log n)          | normalize_string_impl (O(n), score 0.4452)       | 12.0%         |
| perf_1-import_pipeline          | 0               | N/A (unknown)                      | unknown             | N/A (unknown, score N/A)                         | 0.0%          |
| perf_2-post_processing_pipeline | 3               | aggregate_standardized_rows (O(n)) | O(n)                | aggregate_standardized_rows (O(n), score 0.3761) | 70.8%         |
| perf_3-export_pipeline          | 2               | normalize_for_comparison (O(n))    | O(n)                | normalize_for_comparison (O(n), score 0.4662)    | 17.2%         |
```

## Cross-Stage Runtime and Risk Ranking

```text
| Pipeline                        | Runtime share | Stage risk score | Dominant complexity | Top runtime driver                  | Top composite bottleneck             | Low-confidence share | High-volatility share | Critical bottlenecks |
| ------------------------------- | ------------- | ---------------- | ------------------- | ----------------------------------- | ------------------------------------ | -------------------- | --------------------- | -------------------- |
| perf_2-post_processing_pipeline | 70.8%         | 0.4164           | O(n)                | aggregate_standardized_rows (73.2%) | aggregate_standardized_rows (0.3761) | 0.0%                 | 33.3%                 | 0                    |
| perf_3-export_pipeline          | 17.2%         | 0.169            | O(n)                | normalize_for_comparison (98.9%)    | normalize_for_comparison (0.4662)    | 0.0%                 | 0.0%                  | 0                    |
| perf_0-general_pipeline         | 12.0%         | 0.2646           | O(n log n)          | coerce_numeric_safe (40.9%)         | normalize_string_impl (0.4452)       | 0.0%                 | 66.7%                 | 0                    |
| perf_1-import_pipeline          | 0.0%          | 0                | unknown             | N/A (0.0%)                          | N/A (N/A)                            | 0.0%                 | 0.0%                  | 0                    |
```

## Stage Bottleneck Matrix

```text
| Pipeline                        | Runtime driver                      | Asymptotic driver                  | Composite-score driver               | Priority | Stage risk score | Low-confidence fns | High-volatility fns |
| ------------------------------- | ----------------------------------- | ---------------------------------- | ------------------------------------ | -------- | ---------------- | ------------------ | ------------------- |
| perf_0-general_pipeline         | coerce_numeric_safe (40.9%)         | drop_na_value_rows (O(n log n))    | normalize_string_impl (0.4452)       | P2       | 0.2646           | 0                  | 2                   |
| perf_1-import_pipeline          | NA (NA)                             | NA (NA)                            | NA (N/A)                             | N/A      | N/A              | N/A                | N/A                 |
| perf_2-post_processing_pipeline | aggregate_standardized_rows (73.2%) | aggregate_standardized_rows (O(n)) | aggregate_standardized_rows (0.3761) | P2       | 0.4164           | 0                  | 1                   |
| perf_3-export_pipeline          | normalize_for_comparison (98.9%)    | normalize_for_comparison (O(n))    | normalize_for_comparison (0.4662)    | P2       | 0.169            | 0                  | 0                   |
```

## Global Top Bottleneck Functions

```text
| Rank | Function                     | Pipeline                        | Complexity | Composite score | Stage impact | Confidence | Volatility cv | Priority | Flags                       | Likely slowdown drivers                              |
| ---- | ---------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------- | ------------- | -------- | --------------------------- | ---------------------------------------------------- |
| 1    | normalize_for_comparison     | perf_3-export_pipeline          | O(n)       | 0.4662          | 98.9%        | very high  | 0.19625       | P2       | high_impact                 | sorting/reordering work adds comparison overhead; t~ |
| 2    | normalize_string_impl        | perf_0-general_pipeline         | O(n)       | 0.4452          | 35.8%        | very high  | 0.23207       | P2       | high_impact/high_volatility | string normalization/coercion is CPU and allocation~ |
| 3    | aggregate_standardized_rows  | perf_2-post_processing_pipeline | O(n)       | 0.3761          | 73.2%        | very high  | 0.040152      | P2       | high_impact                 | grouping across repeated keys increases hash/scan c~ |
| 4    | coerce_numeric_safe          | perf_0-general_pipeline         | O(n)       | 0.263           | 40.9%        | very high  | 0.026234      | P2       | high_impact                 | string normalization/coercion is CPU and allocation~ |
| 5    | drop_na_value_rows           | perf_0-general_pipeline         | O(n log n) | 0.2623          | 23.4%        | very high  | 0.70219       | P2       | high_volatility             | runtime jumps sharply between sizes (max 23.32x); h~ |
| 6    | apply_standardize_rules      | perf_2-post_processing_pipeline | O(n)       | 0.1769          | 16.1%        | very high  | 0.031353      | P2       | ok                          | grouping across repeated keys increases hash/scan c~ |
| 7    | extract_aggregated_rows      | perf_2-post_processing_pipeline | O(n)       | 0.1576          | 10.7%        | very high  | 0.33865       | P2       | high_volatility             | grouping across repeated keys increases hash/scan c~ |
| 8    | compute_unique_column_values | perf_3-export_pipeline          | O(n)       | 0.124           | 1.1%         | very high  | 0.047962      | P2       | ok                          | sorting/reordering work adds comparison overhead; r~ |
```

## Recommended Optimization Roadmap

### Quick Wins

```text
| Function                    | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| --------------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| normalize_for_comparison    | perf_3-export_pipeline          | O(n)       | 0.4662          | 98.9%        | large runtime share; sharp growth jump (max 13.75x)  | up to 98.9% stage runtime |
| aggregate_standardized_rows | perf_2-post_processing_pipeline | O(n)       | 0.3761          | 73.2%        | large runtime share; sharp growth jump (max 10.03x)  | up to 73.2% stage runtime |
| coerce_numeric_safe         | perf_0-general_pipeline         | O(n)       | 0.263           | 40.9%        | large runtime share; sharp growth jump (max 12.05x)  | up to 40.9% stage runtime |
| normalize_string_impl       | perf_0-general_pipeline         | O(n)       | 0.4452          | 35.8%        | large runtime share; unstable repeated timings; sha~ | up to 35.8% stage runtime |
| drop_na_value_rows          | perf_0-general_pipeline         | O(n log n) | 0.2623          | 23.4%        | unstable repeated timings; sharp growth jump (max 2~ | up to 23.4% stage runtime |
```

### Medium Effort

```text
| Function                | Pipeline                        | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           |
| ----------------------- | ------------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- |
| normalize_string_impl   | perf_0-general_pipeline         | O(n)       | 0.4452          | 35.8%        | large runtime share; unstable repeated timings; sha~ | up to 35.8% stage runtime |
| drop_na_value_rows      | perf_0-general_pipeline         | O(n log n) | 0.2623          | 23.4%        | unstable repeated timings; sharp growth jump (max 2~ | up to 23.4% stage runtime |
| extract_aggregated_rows | perf_2-post_processing_pipeline | O(n)       | 0.1576          | 10.7%        | unstable repeated timings; sharp growth jump (max 8~ | up to 10.7% stage runtime |
```

### High Effort / High Impact

```text
| Function | Pipeline | Complexity | Composite score | Stage impact | Reason                   | Expected impact |
| -------- | -------- | ---------- | --------------- | ------------ | ------------------------ | --------------- |
| N/A      | N/A      | unknown    | N/A             | 0.0%         | No candidates identified | N/A             |
```

## Project Narrative

- Runtime is most concentrated in perf_2-post_processing_pipeline (70.8% of observed cross-stage runtime).
- Asymptotic risk is highest in perf_1-import_pipeline (dominant class unknown).
- Optimize first: normalize_for_comparison in perf_3-export_pipeline (score 0.4662, impact 98.9%).
