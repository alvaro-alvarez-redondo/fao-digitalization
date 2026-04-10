# Pipeline Performance Report: perf_3-export_pipeline

- Stage identifier: 3-export
- Total functions benchmarked: 2
- Dominant complexity class: O(n)
- Stage runtime share: 2.2%
- Stage risk score: 0.1587
- Highest-complexity function: normalize_for_comparison (O(n))
- Primary bottleneck candidate: normalize_for_comparison (O(n), 99.2% impact)
- Runtime projection sample n values: 1000, 100000, 10000000

## Stage KPI Dashboard

```text
| KPI                         | Value                                   |
| --------------------------- | --------------------------------------- |
| Total functions benchmarked | 2                                       |
| Dominant complexity class   | O(n)                                    |
| Stage runtime total (s)     | 11.809                                  |
| Stage runtime share         | 2.2%                                    |
| Expensive function share    | 0.0%                                    |
| Stage risk score            | 0.1587                                  |
| Top runtime driver          | normalize_for_comparison (99.2%)        |
| Top composite bottleneck    | normalize_for_comparison (score 0.4671) |
```

## Function-Level Performance Matrix

```text
| Function                     | Description                                          | Complexity | adj.R2   | Slope per n      | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags           | Dominant in stage | Likely slowdown drivers                              |
| ---------------------------- | ---------------------------------------------------- | ---------- | -------- | ---------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | --------------- | ----------------- | ---------------------------------------------------- |
| normalize_for_comparison     | deep-copy, drop year col, sort columns and rows of ~ | O(n)       | 0.999291 | 0.00000108422    | n=1000: 0.001084 s; n=100000: 0.1084 s; n=10000000:~ | 99.2%           | ! (high_impact) | no         | 3               | very high  | 11.709                     | 10.793               | 0.10912       | 1.264         | 13.09      | 0.4671          | high_impact     | yes               | sorting/reordering work adds comparison overhead; t~ |
| compute_unique_column_values | compute sorted unique values for one column of n-ro~ | O(n)       | 0.999337 | 0.00000000854575 | n=1000: 0.000008546 s; n=100000: 0.0008546 s; n=100~ | 0.8%            | ? (watch)       | no         | 3               | very high  | 0.10011                    | 0.086347             | 1.3965        | 8.431         | 7.83       | 0.1231          | high_volatility | yes               | sorting/reordering work adds comparison overhead; r~ |
```

## Bottleneck Candidates

- normalize_for_comparison: class O(n), score 0.4671, impact 99.2%, flags high_impact.
- compute_unique_column_values: class O(n), score 0.1231, impact 0.8%, flags high_volatility.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                     | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags           | Likely slowdown drivers                              |
| ---- | ---------------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | --------------- | ---------------------------------------------------- |
| 1    | normalize_for_comparison     | O(n)       | 0.4671          | 99.2%        | 0.99929 | very high  | 0.10912       | 13.09      | high_impact     | sorting/reordering work adds comparison overhead; t~ |
| 2    | compute_unique_column_values | O(n)       | 0.1231          | 0.8%         | 0.99934 | very high  | 1.3965        | 7.83       | high_volatility | sorting/reordering work adds comparison overhead; r~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99931 |
| Median adjusted R2          | 0.99931 |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 1       |
| High-volatility share       | 50.0%   |
| Critical bottlenecks        | 0       |
| Runtime concentration (HHI) | 0.98319 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                     | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags           |
| ------------- | ---------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | --------------- |
| P2            | normalize_for_comparison     | O(n)       | 0.4671          | 99.2%        | large runtime share; sharp growth jump (max 13.09x)  | up to 99.2% stage runtime | high_impact     |
| P2            | compute_unique_column_values | O(n)       | 0.1231          | 0.8%         | unstable repeated timings; sharp growth jump (max 7~ | up to 0.8% stage runtime  | high_volatility |
```

## Stage Narrative

- Runtime is dominated by normalize_for_comparison, contributing 99.2% of stage runtime.
- Asymptotic risk is dominated by normalize_for_comparison with class O(n).
- Optimize first: normalize_for_comparison because large runtime share; sharp growth jump (max 13.09x); expected impact up to 99.2% stage runtime.

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
| normalize_for_comparison     | 99.2%           | 0.4671          | ######################## |
| compute_unique_column_values | 0.8%            | 0.1231          | ------------------------ |
```

## Runtime Projection Grid

```text
| Function                     | n        | Estimated runtime (s) |
| ---------------------------- | -------- | --------------------- |
| compute_unique_column_values | 1000     | 0.0000085458          |
| normalize_for_comparison     | 1000     | 0.0010842             |
| compute_unique_column_values | 100000   | 0.00085458            |
| normalize_for_comparison     | 100000   | 0.10842               |
| compute_unique_column_values | 10000000 | 0.085458              |
| normalize_for_comparison     | 10000000 | 10.842                |
```
