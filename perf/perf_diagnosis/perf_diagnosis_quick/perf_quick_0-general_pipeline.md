# Pipeline Performance Report: perf_0-general_pipeline

- Stage identifier: 0-general
- Preset: quick
- Total functions benchmarked: 3
- Dominant complexity class: unknown
- Stage runtime share: 0.0%
- Stage risk score: 0.4334
- Highest-complexity function: normalize_string_impl (unknown)
- Primary bottleneck candidate: normalize_string_impl (unknown, 63.6% impact)
- Runtime projection sample n values: 1000, 10000

## Stage KPI Dashboard

```text
| KPI                         | Value                                |
| --------------------------- | ------------------------------------ |
| Total functions benchmarked | 3                                    |
| Dominant complexity class   | unknown                              |
| Stage runtime total (s)     | 0.0099647                            |
| Stage runtime share         | 0.0%                                 |
| Expensive function share    | 100.0%                               |
| Stage risk score            | 0.4334                               |
| Top runtime driver          | normalize_string_impl (63.6%)        |
| Top composite bottleneck    | normalize_string_impl (score 0.8083) |
```

## Function-Level Performance Matrix

```text
| Function              | Description                                          | Complexity | adj.R2 | Slope per n | Estimated runtime (sample n)  | Relative impact | Indicator            | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags                                                | Dominant in stage | Likely slowdown drivers                              |
| --------------------- | ---------------------------------------------------- | ---------- | ------ | ----------- | ----------------------------- | --------------- | -------------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | ---------------------------------------------------- | ----------------- | ---------------------------------------------------- |
| normalize_string_impl | normalise a character vector of length n to ASCII l~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 63.6%           | !!! (critical)       | yes        | 7               | unknown    | 0.0063389                  | 0.0044464            | 0.071403      | 1.128         | 2.349      | 0.8083          | high_complexity/high_impact/low_confidence/critical~ | yes               | string normalization/coercion is CPU and allocation~ |
| coerce_numeric_safe   | coerce a character vector of length n to numeric     | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 21.5%           | !! (high_complexity) | no         | 7               | unknown    | 0.0021397                  | 0.0018525            | 0.18309       | 1.281         | 6.45       | 0.5252          | high_complexity/low_confidence                       | yes               | string normalization/coercion is CPU and allocation~ |
| drop_na_value_rows    | filter NA-value rows from a data.table of n rows (5~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 14.9%           | !! (high_complexity) | no         | 7               | unknown    | 0.0014861                  | 0.0010336            | 0.19737       | 1.254         | 2.284      | 0.5022          | high_complexity/low_confidence                       | yes               | super-linear class (unknown) raises asymptotic risk~ |
```

## Bottleneck Candidates

- normalize_string_impl: class unknown, score 0.8083, impact 63.6%, flags high_complexity/high_impact/low_confidence/critical_bottleneck.
- coerce_numeric_safe: class unknown, score 0.5252, impact 21.5%, flags high_complexity/low_confidence.
- drop_na_value_rows: class unknown, score 0.5022, impact 14.9%, flags high_complexity/low_confidence.

## Top Bottlenecks by Composite Score

```text
| Rank | Function              | Complexity | Composite score | Stage impact | adj.R2 | Confidence | Volatility cv | Growth max | Flags                                                | Likely slowdown drivers                              |
| ---- | --------------------- | ---------- | --------------- | ------------ | ------ | ---------- | ------------- | ---------- | ---------------------------------------------------- | ---------------------------------------------------- |
| 1    | normalize_string_impl | unknown    | 0.8083          | 63.6%        | N/A    | unknown    | 0.071403      | 2.349      | high_complexity/high_impact/low_confidence/critical~ | string normalization/coercion is CPU and allocation~ |
| 2    | coerce_numeric_safe   | unknown    | 0.5252          | 21.5%        | N/A    | unknown    | 0.18309       | 6.45       | high_complexity/low_confidence                       | string normalization/coercion is CPU and allocation~ |
| 3    | drop_na_value_rows    | unknown    | 0.5022          | 14.9%        | N/A    | unknown    | 0.19737       | 2.284      | high_complexity/low_confidence                       | super-linear class (unknown) raises asymptotic risk~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | N/A     |
| Median adjusted R2          | N/A     |
| Low-confidence functions    | 3       |
| Low-confidence share        | 100.0%  |
| High-volatility functions   | 0       |
| High-volatility share       | 0.0%    |
| Critical bottlenecks        | 1       |
| Runtime concentration (HHI) | 0.47302 |
```

## Optimization Priority Queue

```text
| Priority tier | Function              | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags                                                |
| ------------- | --------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | ---------------------------------------------------- |
| P0            | normalize_string_impl | unknown    | 0.8083          | 63.6%        | large runtime share; super-linear growth risk; low ~ | up to 63.6% stage runtime | high_complexity/high_impact/low_confidence/critical~ |
| P2            | coerce_numeric_safe   | unknown    | 0.5252          | 21.5%        | super-linear growth risk; low model confidence; sha~ | up to 21.5% stage runtime | high_complexity/low_confidence                       |
| P2            | drop_na_value_rows    | unknown    | 0.5022          | 14.9%        | super-linear growth risk; low model confidence; sha~ | up to 14.9% stage runtime | high_complexity/low_confidence                       |
```

## Stage Narrative

- Runtime is dominated by normalize_string_impl, contributing 63.6% of stage runtime.
- Asymptotic risk is dominated by normalize_string_impl with class unknown.
- Optimize first: normalize_string_impl because large runtime share; super-linear growth risk; low model confidence; sharp growth jump (max 2.35x); expected impact up to 63.6% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| unknown          | 3              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function              | Relative impact | Composite score | Distribution             |
| --------------------- | --------------- | --------------- | ------------------------ |
| normalize_string_impl | 63.6%           | 0.8083          | ###############--------- |
| coerce_numeric_safe   | 21.5%           | 0.5252          | #####------------------- |
| drop_na_value_rows    | 14.9%           | 0.5022          | ####-------------------- |
```

## Runtime Projection Grid

```text
| Function              | n     | Estimated runtime (s) |
| --------------------- | ----- | --------------------- |
| coerce_numeric_safe   | 1000  | N/A                   |
| drop_na_value_rows    | 1000  | N/A                   |
| normalize_string_impl | 1000  | N/A                   |
| coerce_numeric_safe   | 10000 | N/A                   |
| drop_na_value_rows    | 10000 | N/A                   |
| normalize_string_impl | 10000 | N/A                   |
```
