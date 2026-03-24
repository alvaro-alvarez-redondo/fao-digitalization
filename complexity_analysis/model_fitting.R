# complexity_analysis/model_fitting.R
# description: model fitting layer for the Big O complexity analysis module.
#   defines the candidate model set and the fitting routine that selects the
#   best model by adjusted R². candidate models cover O(1) through O(n^3) so
#   that the framework is extensible — new models can be added to
#   get_complexity_candidates() without touching fit_complexity_model().
#
# sourced by: complexity_analysis/run_analysis.R

# ── model fitting layer ──────────────────────────────────────────────────────

#' @title complexity model candidates
#' @description returns a list of candidate complexity model descriptors.
#'   each entry has a `label` and a function `f(n)` that computes the
#'   predictor variable for a linear regression `t ~ f(n)`.
#'   to add a new model, append an entry to this list.
#' @return list of named lists with fields `label` and `f`.
get_complexity_candidates <- function() {
  list(
    # O(1): predictor is all-zero so lm() falls through to intercept-only;
    # slope is not meaningful for O(1).
    list(label = "O(1)",       f = function(n) rep(0, length(n))),
    list(label = "O(log n)",   f = function(n) log(n)),
    list(label = "O(n)",       f = function(n) n),
    list(label = "O(n log n)", f = function(n) n * log(n)),
    list(label = "O(n^2)",     f = function(n) n^2),
    list(label = "O(n^3)",     f = function(n) n^3)
  )
}

#' @title fit complexity model
#' @description fits each candidate complexity model via linear regression and
#'   returns the best-fitting class (by adjusted R²) together with all R²
#'   values and the fitted slope.
#' @param n_values numeric vector of input sizes.
#' @param t_values numeric vector of corresponding median elapsed times (seconds).
#' @return named list:
#'   \describe{
#'     \item{best_class}{character scalar — best-fitting complexity label}
#'     \item{best_r2}{numeric — adjusted R² of best model (NA if insufficient data)}
#'     \item{all_r2}{named numeric vector of adjusted R² per candidate}
#'     \item{slope_per_n}{numeric — fitted slope for best model per unit n}
#'   }
fit_complexity_model <- function(n_values, t_values) {
  valid <- !is.na(t_values) & !is.na(n_values) &
    is.finite(t_values) & is.finite(n_values) &
    t_values >= 0 & n_values > 0
  n <- as.numeric(n_values[valid])
  t <- as.numeric(t_values[valid])

  candidates <- get_complexity_candidates()
  fallback <- list(
    best_class  = "unknown",
    best_r2     = NA_real_,
    all_r2      = setNames(rep(NA_real_, length(candidates)),
                           vapply(candidates, function(m) m$label, character(1))),
    slope_per_n = NA_real_
  )

  if (length(n) < 3L) {
    return(fallback)
  }

  fit_results <- lapply(candidates, function(m) {
    x   <- m$f(n)
    # O(1): predictor is all-zero, use intercept-only model
    if (all(x == 0)) {
      fit <- tryCatch(stats::lm(t ~ 1), error = function(e) NULL)
    } else {
      df  <- data.frame(t = t, x = x)
      fit <- tryCatch(stats::lm(t ~ x, data = df), error = function(e) NULL)
    }
    if (is.null(fit)) return(list(label = m$label, r2 = -Inf, slope = NA_real_))
    summ  <- summary(fit)
    r2    <- if (is.na(summ$adj.r.squared)) -Inf else summ$adj.r.squared
    coef  <- stats::coef(fit)
    slope <- if (length(coef) >= 2L) unname(coef[2L]) else NA_real_
    list(label = m$label, r2 = r2, slope = slope)
  })

  r2_vec    <- vapply(fit_results, function(x) x$r2,    numeric(1))
  slope_vec <- vapply(fit_results, function(x) x$slope, numeric(1))
  names(r2_vec)    <- vapply(candidates, function(m) m$label, character(1))
  names(slope_vec) <- names(r2_vec)

  best_idx <- which.max(r2_vec)
  list(
    best_class  = fit_results[[best_idx]]$label,
    best_r2     = r2_vec[[best_idx]],
    all_r2      = r2_vec,
    slope_per_n = slope_vec[[best_idx]]
  )
}
