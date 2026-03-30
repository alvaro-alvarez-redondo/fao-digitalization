#' @title Complexity model module
#' @description Complexity-class candidate definitions, model fitting helpers,
#'   and ranking utilities used by the performance framework.
#' @keywords internal
#' @noRd
NULL

# ── 3. complexity model fitting ──────────────────────────────────────────────

#' @title Complexity class ordering
#' @description Internal named integer vector mapping each complexity class to
#'   an ordinal rank. Higher rank means more expensive complexity.
#' @keywords internal
#' @noRd
.complexity_order <- c(
  "O(1)" = 1L,
  "O(log n)" = 2L,
  "O(n)" = 3L,
  "O(n log n)" = 4L,
  "O(n^2)" = 5L,
  "O(n^3)" = 6L,
  "unknown" = 7L,
  "ERROR" = 8L
)

#' @title Get complexity model candidates
#' @description Return candidate model descriptors used in empirical complexity
#'   fitting.
#' @return A list of descriptors with fields label (character) and f (function).
get_complexity_candidates <- function() {
  list(
    list(label = "O(1)", f = function(n) rep(0, length(n))),
    list(label = "O(log n)", f = function(n) log(n)),
    list(label = "O(n)", f = function(n) n),
    list(label = "O(n log n)", f = function(n) n * log(n)),
    list(label = "O(n^2)", f = function(n) n^2),
    list(label = "O(n^3)", f = function(n) n^3)
  )
}

#' @title Rank complexity class
#' @description Map a complexity class label to its integer rank.
#' @param class_label A character scalar naming a complexity class.
#' @return An integer scalar rank.
complexity_rank_value <- function(class_label) {
  unname(.complexity_order[class_label])
}

#' @title Fit complexity model
#' @description Fit each candidate complexity model and return the best class
#'   by adjusted R-squared.
#' @param n_values A numeric vector of input sizes.
#' @param t_values A numeric vector of elapsed times corresponding to n_values.
#' @return A named list:
#'   \describe{
#'     \item{best_class}{character scalar — best-fitting complexity label}
#'     \item{best_r2}{numeric — adjusted R² of best model (NA if insufficient data)}
#'     \item{all_r2}{named numeric vector of adjusted R² per candidate}
#'     \item{slope_per_n}{numeric — fitted slope for best model per unit n}
#'   }
fit_complexity_model <- function(n_values, t_values) {
  valid <- !is.na(t_values) &
    !is.na(n_values) &
    is.finite(t_values) &
    is.finite(n_values) &
    t_values >= 0 &
    n_values > 0
  n <- as.numeric(n_values[valid])
  t <- as.numeric(t_values[valid])

  candidate_labels <- vapply(
    get_complexity_candidates(),
    function(m) m$label,
    character(1)
  )
  fallback <- list(
    best_class = "unknown",
    best_r2 = NA_real_,
    all_r2 = setNames(
      rep(NA_real_, length(candidate_labels)),
      candidate_labels
    ),
    slope_per_n = NA_real_
  )

  if (length(n) < 3L) {
    return(fallback)
  }

  candidates <- get_complexity_candidates()
  fit_results <- lapply(candidates, function(m) {
    x <- m$f(n)
    if (all(x == 0)) {
      # O(1): intercept-only model; slope is not meaningful
      fit <- tryCatch(stats::lm(t ~ 1), error = function(e) NULL)
    } else {
      df <- data.frame(t = t, x = x)
      fit <- tryCatch(stats::lm(t ~ x, data = df), error = function(e) NULL)
    }
    if (is.null(fit)) {
      return(list(label = m$label, r2 = -Inf, slope = NA_real_))
    }
    summ <- summary(fit)
    r2 <- if (is.na(summ$adj.r.squared)) -Inf else summ$adj.r.squared
    coef <- stats::coef(fit)
    slope <- if (length(coef) >= 2L) unname(coef[2L]) else NA_real_
    list(label = m$label, r2 = r2, slope = slope)
  })

  r2_vec <- vapply(fit_results, function(x) x$r2, numeric(1))
  slope_vec <- vapply(fit_results, function(x) x$slope, numeric(1))
  names(r2_vec) <- vapply(candidates, function(m) m$label, character(1))
  names(slope_vec) <- names(r2_vec)

  best_idx <- which.max(r2_vec)
  list(
    best_class = fit_results[[best_idx]]$label,
    best_r2 = r2_vec[[best_idx]],
    all_r2 = r2_vec,
    slope_per_n = slope_vec[[best_idx]]
  )
}
