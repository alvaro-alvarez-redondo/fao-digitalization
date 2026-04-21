# Pipeline Performance Report: perf_3-export_pipeline

- Stage identifier: 3-export
- Preset: targeted
- Total functions benchmarked: 2
- Dominant complexity class: O(n)
- Stage runtime share: 17.2%
- Stage risk score: 0.169
- Highest-complexity function: normalize_for_comparison (O(n))
- Primary bottleneck candidate: normalize_for_comparison (O(n), 98.9% impact)
- Runtime projection sample n values: 1000, 10000, 1000000

## Stage KPI Dashboard

```text
| KPI                         | Value                                   |
| --------------------------- | --------------------------------------- |
| Total functions benchmarked | 2                                       |
| Dominant complexity class   | O(n)                                    |
| Stage runtime total (s)     | 0.90338                                 |
| Stage runtime share         | 17.2%                                   |
| Expensive function share    | 0.0%                                    |
| Stage risk score            | 0.169                                   |
| Top runtime driver          | normalize_for_comparison (98.9%)        |
| Top composite bottleneck    | normalize_for_comparison (score 0.4662) |
```

## Function-Level Performance Matrix

```text
| Function                     | Description                                          | Complexity | adj.R2   | Slope per n      | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags       | Dominant in stage | Likely slowdown drivers                              |
| ---------------------------- | ---------------------------------------------------- | ---------- | -------- | ---------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | ----------- | ----------------- | ---------------------------------------------------- |
| normalize_for_comparison     | deep-copy, drop year col, sort columns and rows of ~ | O(n)       | 0.998884 | 0.00000082981    | n=1000: 0.0008298 s; n=10000: 0.008298 s; n=1000000~ | 98.9%           | ! (high_impact) | no         | 3               | very high  | 0.89317                    | 0.82414              | 0.19625       | 1.284         | 13.75      | 0.4662          | high_impact | yes               | sorting/reordering work adds comparison overhead; t~ |
| compute_unique_column_values | compute sorted unique values for one column of n-ro~ | O(n)       | 0.999492 | 0.00000000793572 | n=1000: 0.000007936 s; n=10000: 0.00007936 s; n=100~ | 1.1%            | . (ok)          | no         | 3               | very high  | 0.010217                   | 0.0082769            | 0.047962      | 1.01          | 6.719      | 0.124           | ok          | yes               | sorting/reordering work adds comparison overhead; r~ |
```

## Bottleneck Candidates

- normalize_for_comparison: class O(n), score 0.4662, impact 98.9%, flags high_impact.
- compute_unique_column_values: class O(n), score 0.124, impact 1.1%, flags ok.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                     | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags       | Likely slowdown drivers                              |
| ---- | ---------------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | ----------- | ---------------------------------------------------- |
| 1    | normalize_for_comparison     | O(n)       | 0.4662          | 98.9%        | 0.99888 | very high  | 0.19625       | 13.75      | high_impact | sorting/reordering work adds comparison overhead; t~ |
| 2    | compute_unique_column_values | O(n)       | 0.124           | 1.1%         | 0.99949 | very high  | 0.047962      | 6.719      | ok          | sorting/reordering work adds comparison overhead; r~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99919 |
| Median adjusted R2          | 0.99919 |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 0       |
| High-volatility share       | 0.0%    |
| Critical bottlenecks        | 0       |
| Runtime concentration (HHI) | 0.97764 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                     | Complexity | Composite score | Stage impact | Reason                                              | Expected impact           | Flags       |
| ------------- | ---------------------------- | ---------- | --------------- | ------------ | --------------------------------------------------- | ------------------------- | ----------- |
| P2            | normalize_for_comparison     | O(n)       | 0.4662          | 98.9%        | large runtime share; sharp growth jump (max 13.75x) | up to 98.9% stage runtime | high_impact |
| P2            | compute_unique_column_values | O(n)       | 0.124           | 1.1%         | sharp growth jump (max 6.72x)                       | up to 1.1% stage runtime  | ok          |
```

## Stage Narrative

- Runtime is dominated by normalize_for_comparison, contributing 98.9% of stage runtime.
- Asymptotic risk is dominated by normalize_for_comparison with class O(n).
- Optimize first: normalize_for_comparison because large runtime share; sharp growth jump (max 13.75x); expected impact up to 98.9% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| O(n)             | 2              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function                     | Relative impact | Composite score | Distribution             |
| ---------------------------- | --------------- | --------------- | ------------------------ |
| normalize_for_comparison     | 98.9%           | 0.4662          | ######################## |
| compute_unique_column_values | 1.1%            | 0.124           | ------------------------ |
```

## Runtime Projection Grid

```text
| Function                     | n       | Estimated runtime (s) |
| ---------------------------- | ------- | --------------------- |
| compute_unique_column_values | 1000    | 0.0000079357          |
| normalize_for_comparison     | 1000    | 0.00082981            |
| compute_unique_column_values | 10000   | 0.000079357           |
| normalize_for_comparison     | 10000   | 0.0082981             |
| compute_unique_column_values | 1000000 | 0.0079357             |
| normalize_for_comparison     | 1000000 | 0.82981               |
```
