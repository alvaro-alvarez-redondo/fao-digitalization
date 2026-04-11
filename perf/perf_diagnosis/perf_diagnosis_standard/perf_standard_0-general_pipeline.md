# Pipeline Performance Report: perf_0-general_pipeline

- Stage identifier: 0-general
- Preset: standard
- Total functions benchmarked: 3
- Dominant complexity class: O(n^2)
- Stage runtime share: 1.5%
- Stage risk score: 0.2395
- Highest-complexity function: drop_na_value_rows (O(n^2))
- Primary bottleneck candidate: drop_na_value_rows (O(n^2), 35.2% impact)
- Runtime projection sample n values: 1000, 100000, 10000000

## Stage KPI Dashboard

```text
| KPI                         | Value                             |
| --------------------------- | --------------------------------- |
| Total functions benchmarked | 3                                 |
| Dominant complexity class   | O(n^2)                            |
| Stage runtime total (s)     | 8.3476                            |
| Stage runtime share         | 1.5%                              |
| Expensive function share    | 33.3%                             |
| Stage risk score            | 0.2395                            |
| Top runtime driver          | coerce_numeric_safe (38.3%)       |
| Top composite bottleneck    | drop_na_value_rows (score 0.3632) |
```

## Function-Level Performance Matrix

```text
| Function              | Description                                          | Complexity | adj.R2   | Slope per n           | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags                                           | Dominant in stage | Likely slowdown drivers                              |
| --------------------- | ---------------------------------------------------- | ---------- | -------- | --------------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | ----------------------------------------------- | ----------------- | ---------------------------------------------------- |
| drop_na_value_rows    | filter NA-value rows from a data.table of n rows (5~ | O(n^2)     | 0.999939 | 0.0000000000000287238 | n=1000: 0.00000002872 s; n=100000: 0.0002872 s; n=1~ | 35.2%           | !!! (critical)  | yes        | 5               | very high  | 2.9379                     | 2.8795               | 0.076319      | 1.033         | 56.82      | 0.3632          | high_complexity/high_impact/critical_bottleneck | yes               | super-linear class (O(n^2)) raises asymptotic risk;~ |
| normalize_string_impl | normalise a character vector of length n to ASCII l~ | O(n)       | 0.999999 | 0.000000197891        | n=1000: 0.0001979 s; n=100000: 0.01979 s; n=1000000~ | 26.5%           | . (ok)          | no         | 3               | very high  | 2.2101                     | 1.9811               | 0.029774      | 1.055         | 9.874      | 0.3276          | ok                                              | no                | string normalization/coercion is CPU and allocation~ |
| coerce_numeric_safe   | coerce a character vector of length n to numeric     | O(n)       | 0.999682 | 0.000000294279        | n=1000: 0.0002943 s; n=100000: 0.02943 s; n=1000000~ | 38.3%           | ! (high_impact) | no         | 3               | very high  | 3.1997                     | 2.9322               | 0.040136      | 1.111         | 12.58      | 0.2542          | high_impact                                     | no                | string normalization/coercion is CPU and allocation~ |
```

## Bottleneck Candidates

- drop_na_value_rows: class O(n^2), score 0.3632, impact 35.2%, flags high_complexity/high_impact/critical_bottleneck.
- normalize_string_impl: class O(n), score 0.3276, impact 26.5%, flags ok.
- coerce_numeric_safe: class O(n), score 0.2542, impact 38.3%, flags high_impact.

## Top Bottlenecks by Composite Score

```text
| Rank | Function              | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags                                           | Likely slowdown drivers                              |
| ---- | --------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | ----------------------------------------------- | ---------------------------------------------------- |
| 1    | drop_na_value_rows    | O(n^2)     | 0.3632          | 35.2%        | 0.99994 | very high  | 0.076319      | 56.82      | high_complexity/high_impact/critical_bottleneck | super-linear class (O(n^2)) raises asymptotic risk;~ |
| 2    | normalize_string_impl | O(n)       | 0.3276          | 26.5%        | 1       | very high  | 0.029774      | 9.874      | ok                                              | string normalization/coercion is CPU and allocation~ |
| 3    | coerce_numeric_safe   | O(n)       | 0.2542          | 38.3%        | 0.99968 | very high  | 0.040136      | 12.58      | high_impact                                     | string normalization/coercion is CPU and allocation~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99987 |
| Median adjusted R2          | 0.99994 |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 0       |
| High-volatility share       | 0.0%    |
| Critical bottlenecks        | 1       |
| Runtime concentration (HHI) | 0.34088 |
```

## Optimization Priority Queue

```text
| Priority tier | Function              | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags                                           |
| ------------- | --------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | ----------------------------------------------- |
| P0            | drop_na_value_rows    | O(n^2)     | 0.3632          | 35.2%        | large runtime share; super-linear growth risk; shar~ | up to 35.2% stage runtime | high_complexity/high_impact/critical_bottleneck |
| P2            | normalize_string_impl | O(n)       | 0.3276          | 26.5%        | sharp growth jump (max 9.87x)                        | up to 26.5% stage runtime | ok                                              |
| P2            | coerce_numeric_safe   | O(n)       | 0.2542          | 38.3%        | large runtime share; sharp growth jump (max 12.58x)  | up to 38.3% stage runtime | high_impact                                     |
```

## Stage Narrative

- Runtime is dominated by coerce_numeric_safe, contributing 38.3% of stage runtime.
- Asymptotic risk is dominated by drop_na_value_rows with class O(n^2).
- Optimize first: drop_na_value_rows because large runtime share; super-linear growth risk; sharp growth jump (max 56.82x); expected impact up to 35.2% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| O(n)             | 2              | 66.7%          | 64.8%         | ################-------- |
| O(n^2)           | 1              | 33.3%          | 35.2%         | ########---------------- |
```

## Runtime Share Distribution (ASCII)

```text
| Function              | Relative impact | Composite score | Distribution             |
| --------------------- | --------------- | --------------- | ------------------------ |
| coerce_numeric_safe   | 38.3%           | 0.2542          | #########--------------- |
| drop_na_value_rows    | 35.2%           | 0.3632          | ########---------------- |
| normalize_string_impl | 26.5%           | 0.3276          | ######------------------ |
```

## Runtime Projection Grid

```text
| Function              | n        | Estimated runtime (s) |
| --------------------- | -------- | --------------------- |
| coerce_numeric_safe   | 1000     | 0.00029428            |
| drop_na_value_rows    | 1000     | 0.000000028724        |
| normalize_string_impl | 1000     | 0.00019789            |
| coerce_numeric_safe   | 100000   | 0.029428              |
| drop_na_value_rows    | 100000   | 0.00028724            |
| normalize_string_impl | 100000   | 0.019789              |
| coerce_numeric_safe   | 10000000 | 2.9428                |
| drop_na_value_rows    | 10000000 | 2.8724                |
| normalize_string_impl | 10000000 | 1.9789                |
```
