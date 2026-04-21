# Pipeline Performance Report: perf_2-post_processing_pipeline

- Stage identifier: 2-post_processing
- Preset: targeted
- Total functions benchmarked: 3
- Dominant complexity class: O(n)
- Stage runtime share: 70.8%
- Stage risk score: 0.4164
- Highest-complexity function: aggregate_standardized_rows (O(n))
- Primary bottleneck candidate: aggregate_standardized_rows (O(n), 73.2% impact)
- Runtime projection sample n values: 1000, 10000, 1000000

## Stage KPI Dashboard

```text
| KPI                         | Value                                      |
| --------------------------- | ------------------------------------------ |
| Total functions benchmarked | 3                                          |
| Dominant complexity class   | O(n)                                       |
| Stage runtime total (s)     | 3.7068                                     |
| Stage runtime share         | 70.8%                                      |
| Expensive function share    | 0.0%                                       |
| Stage risk score            | 0.4164                                     |
| Top runtime driver          | aggregate_standardized_rows (73.2%)        |
| Top composite bottleneck    | aggregate_standardized_rows (score 0.3761) |
```

## Function-Level Performance Matrix

```text
| Function                    | Description                                          | Complexity | adj.R2   | Slope per n    | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags           | Dominant in stage | Likely slowdown drivers                              |
| --------------------------- | ---------------------------------------------------- | ---------- | -------- | -------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | --------------- | ----------------- | ---------------------------------------------------- |
| aggregate_standardized_rows | group-sum n rows by all non-value columns (2% dupli~ | O(n)       | 0.999996 | 0.0000024337   | n=1000: 0.002434 s; n=10000: 0.02434 s; n=1000000: ~ | 73.2%           | ! (high_impact) | no         | 3               | very high  | 2.7128                     | 2.4362               | 0.040152      | 1.043         | 10.03      | 0.3761          | high_impact     | yes               | grouping across repeated keys increases hash/scan c~ |
| apply_standardize_rules     | apply prepared unit conversion rules to an n-row lo~ | O(n)       | 0.996421 | 0.000000552246 | n=1000: 0.0005522 s; n=10000: 0.005522 s; n=1000000~ | 16.1%           | . (ok)          | no         | 3               | very high  | 0.5967                     | 0.54984              | 0.031353      | 1.007         | 17.17      | 0.1769          | ok              | yes               | grouping across repeated keys increases hash/scan c~ |
| extract_aggregated_rows     | extract duplicate groups from n-row standardized ta~ | O(n)       | 0.999628 | 0.000000319655 | n=1000: 0.0003197 s; n=10000: 0.003197 s; n=1000000~ | 10.7%           | ? (watch)       | no         | 3               | very high  | 0.39733                    | 0.33032              | 0.33865       | 1.924         | 8.059      | 0.1576          | high_volatility | yes               | grouping across repeated keys increases hash/scan c~ |
```

## Bottleneck Candidates

- aggregate_standardized_rows: class O(n), score 0.3761, impact 73.2%, flags high_impact.
- apply_standardize_rules: class O(n), score 0.1769, impact 16.1%, flags ok.
- extract_aggregated_rows: class O(n), score 0.1576, impact 10.7%, flags high_volatility.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                    | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags           | Likely slowdown drivers                              |
| ---- | --------------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | --------------- | ---------------------------------------------------- |
| 1    | aggregate_standardized_rows | O(n)       | 0.3761          | 73.2%        | 1       | very high  | 0.040152      | 10.03      | high_impact     | grouping across repeated keys increases hash/scan c~ |
| 2    | apply_standardize_rules     | O(n)       | 0.1769          | 16.1%        | 0.99642 | very high  | 0.031353      | 17.17      | ok              | grouping across repeated keys increases hash/scan c~ |
| 3    | extract_aggregated_rows     | O(n)       | 0.1576          | 10.7%        | 0.99963 | very high  | 0.33865       | 8.059      | high_volatility | grouping across repeated keys increases hash/scan c~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99868 |
| Median adjusted R2          | 0.99963 |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 1       |
| High-volatility share       | 33.3%   |
| Critical bottlenecks        | 0       |
| Runtime concentration (HHI) | 0.57299 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                    | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags           |
| ------------- | --------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | --------------- |
| P2            | aggregate_standardized_rows | O(n)       | 0.3761          | 73.2%        | large runtime share; sharp growth jump (max 10.03x)  | up to 73.2% stage runtime | high_impact     |
| P2            | apply_standardize_rules     | O(n)       | 0.1769          | 16.1%        | sharp growth jump (max 17.17x)                       | up to 16.1% stage runtime | ok              |
| P2            | extract_aggregated_rows     | O(n)       | 0.1576          | 10.7%        | unstable repeated timings; sharp growth jump (max 8~ | up to 10.7% stage runtime | high_volatility |
```

## Stage Narrative

- Runtime is dominated by aggregate_standardized_rows, contributing 73.2% of stage runtime.
- Asymptotic risk is dominated by aggregate_standardized_rows with class O(n).
- Optimize first: aggregate_standardized_rows because large runtime share; sharp growth jump (max 10.03x); expected impact up to 73.2% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| O(n)             | 3              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function                    | Relative impact | Composite score | Distribution             |
| --------------------------- | --------------- | --------------- | ------------------------ |
| aggregate_standardized_rows | 73.2%           | 0.3761          | ##################------ |
| apply_standardize_rules     | 16.1%           | 0.1769          | ####-------------------- |
| extract_aggregated_rows     | 10.7%           | 0.1576          | ###--------------------- |
```

## Runtime Projection Grid

```text
| Function                    | n       | Estimated runtime (s) |
| --------------------------- | ------- | --------------------- |
| aggregate_standardized_rows | 1000    | 0.0024337             |
| apply_standardize_rules     | 1000    | 0.00055225            |
| extract_aggregated_rows     | 1000    | 0.00031966            |
| aggregate_standardized_rows | 10000   | 0.024337              |
| apply_standardize_rules     | 10000   | 0.0055225             |
| extract_aggregated_rows     | 10000   | 0.0031966             |
| aggregate_standardized_rows | 1000000 | 2.4337                |
| apply_standardize_rules     | 1000000 | 0.55225               |
| extract_aggregated_rows     | 1000000 | 0.31966               |
```
