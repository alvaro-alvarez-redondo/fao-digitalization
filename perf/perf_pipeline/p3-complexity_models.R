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

#' @title Resolve complexity fit tolerance
#' @description Resolve the adjusted R-squared tolerance used to prefer simpler
#'   complexity classes when model fits are effectively tied.
#' @param r2_tolerance Optional numeric scalar override.
#' @return Numeric scalar non-negative tolerance.
#' @keywords internal
#' @noRd
.resolve_complexity_r2_tolerance <- function(r2_tolerance = NULL) {
  if (!is.null(r2_tolerance)) {
    tol <- suppressWarnings(as.numeric(r2_tolerance))
    if (length(tol) != 1L || is.na(tol) || !is.finite(tol) || tol < 0) {
      stop("r2_tolerance must be a non-negative finite numeric scalar")
    }
    return(tol)
  }

  cfg_tol <- NULL
  if (exists("get_analysis_config", mode = "function")) {
    cfg <- tryCatch(get_analysis_config(), error = function(e) NULL)
    if (!is.null(cfg) && is.list(cfg) && !is.null(cfg$complexity_r2_tolerance)) {
      cfg_tol <- cfg$complexity_r2_tolerance
    }
  }

  tol <- suppressWarnings(as.numeric(cfg_tol))
  if (length(tol) != 1L || is.na(tol) || !is.finite(tol) || tol < 0) {
    return(0)
  }
  return(tol)
}

#' @title Resolve minimum acceptable best-fit adjusted R-squared
#' @description Resolve the minimum adjusted R-squared required for a
#'   complexity class assignment to be considered reliable.
#' @param min_best_r2 Optional numeric scalar override.
#' @return Numeric scalar in [-1, 1].
#' @keywords internal
#' @noRd
.resolve_complexity_min_best_r2 <- function(min_best_r2 = NULL) {
  if (!is.null(min_best_r2)) {
    min_r2 <- suppressWarnings(as.numeric(min_best_r2))
    if (
      length(min_r2) != 1L ||
        is.na(min_r2) ||
        !is.finite(min_r2) ||
        min_r2 < -1 ||
        min_r2 > 1
    ) {
      stop("min_best_r2 must be a finite numeric scalar between -1 and 1")
    }
    return(min_r2)
  }

  cfg_value <- NULL
  if (exists("get_analysis_config", mode = "function")) {
    cfg <- tryCatch(get_analysis_config(), error = function(e) NULL)
    if (!is.null(cfg) && is.list(cfg) && !is.null(cfg$complexity_min_best_r2)) {
      cfg_value <- cfg$complexity_min_best_r2
    }
  }

  min_r2 <- suppressWarnings(as.numeric(cfg_value))
  if (
    length(min_r2) != 1L ||
      is.na(min_r2) ||
      !is.finite(min_r2) ||
      min_r2 < -1 ||
      min_r2 > 1
  ) {
    return(0)
  }
  return(min_r2)
}

#' @title Resolve complexity minimum unique input sizes
#' @description Resolve the minimum number of distinct n values required before
#'   attempting complexity fitting.
#' @param min_unique_n Optional integer scalar override.
#' @return Integer scalar >= 3.
#' @keywords internal
#' @noRd
.resolve_complexity_min_unique_n <- function(min_unique_n = NULL) {
  if (!is.null(min_unique_n)) {
    min_n <- suppressWarnings(as.integer(min_unique_n))
    if (length(min_n) != 1L || is.na(min_n) || min_n < 3L) {
      stop("min_unique_n must be an integer scalar >= 3")
    }
    return(min_n)
  }

  cfg_value <- NULL
  if (exists("get_analysis_config", mode = "function")) {
    cfg <- tryCatch(get_analysis_config(), error = function(e) NULL)
    if (!is.null(cfg) && is.list(cfg) && !is.null(cfg$complexity_min_unique_n)) {
      cfg_value <- cfg$complexity_min_unique_n
    }
  }

  min_n <- suppressWarnings(as.integer(cfg_value))
  if (length(min_n) != 1L || is.na(min_n) || min_n < 3L) {
    return(3L)
  }
  return(min_n)
}

#' @title Resolve complexity minimum n-span ratio
#' @description Resolve the minimum required span ratio
#'   max(n) / min(n) for stable complexity fitting.
#' @param min_n_span_ratio Optional numeric scalar override.
#' @return Numeric scalar > 1.
#' @keywords internal
#' @noRd
.resolve_complexity_min_n_span_ratio <- function(min_n_span_ratio = NULL) {
  if (!is.null(min_n_span_ratio)) {
    min_ratio <- suppressWarnings(as.numeric(min_n_span_ratio))
    if (
      length(min_ratio) != 1L ||
        is.na(min_ratio) ||
        !is.finite(min_ratio) ||
        min_ratio <= 1
    ) {
      stop("min_n_span_ratio must be a finite numeric scalar > 1")
    }
    return(min_ratio)
  }

  cfg_value <- NULL
  if (exists("get_analysis_config", mode = "function")) {
    cfg <- tryCatch(get_analysis_config(), error = function(e) NULL)
    if (!is.null(cfg) && is.list(cfg) && !is.null(cfg$complexity_min_n_span_ratio)) {
      cfg_value <- cfg$complexity_min_n_span_ratio
    }
  }

  min_ratio <- suppressWarnings(as.numeric(cfg_value))
  if (
    length(min_ratio) != 1L ||
      is.na(min_ratio) ||
      !is.finite(min_ratio) ||
      min_ratio <= 1
  ) {
    return(2)
  }
  return(min_ratio)
}

#' @title Fit complexity model
#' @description Fit each candidate complexity model and return the best class
#'   by adjusted R-squared.
#' @param n_values A numeric vector of input sizes.
#' @param t_values A numeric vector of elapsed times corresponding to n_values.
#' @param r2_tolerance Optional numeric scalar tolerance for adjusted R-squared
#'   tie handling. When candidate fits are within this tolerance of the best
#'   adjusted R-squared, the simpler class is selected.
#' @param min_best_r2 Optional numeric scalar minimum adjusted R-squared
#'   threshold required to assign a complexity class.
#' @param min_unique_n Optional integer scalar minimum number of distinct
#'   input-size values required before fitting.
#' @param min_n_span_ratio Optional numeric scalar minimum span ratio
#'   max(n) / min(n) required before fitting.
#' @return A named list:
#'   \describe{
#'     \item{best_class}{character scalar — best-fitting complexity label}
#'     \item{best_r2}{numeric — adjusted R² of best model (NA if insufficient data)}
#'     \item{all_r2}{named numeric vector of adjusted R² per candidate}
#'     \item{slope_per_n}{numeric — fitted slope for best model per unit n}
#'   }
fit_complexity_model <- function(
  n_values,
  t_values,
  r2_tolerance = NULL,
  min_best_r2 = NULL,
  min_unique_n = NULL,
  min_n_span_ratio = NULL
) {
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

  r2_tol <- .resolve_complexity_r2_tolerance(r2_tolerance)
  min_best_r2_req <- .resolve_complexity_min_best_r2(min_best_r2)
  min_unique_n_req <- .resolve_complexity_min_unique_n(min_unique_n)
  min_n_span_ratio_req <- .resolve_complexity_min_n_span_ratio(min_n_span_ratio)

  unique_n <- sort(unique(n))
  if (length(unique_n) < min_unique_n_req) {
    return(fallback)
  }

  n_span_ratio <- max(unique_n) / min(unique_n)
  if (!is.finite(n_span_ratio) || n_span_ratio < min_n_span_ratio_req) {
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

  finite_r2 <- is.finite(r2_vec)
  if (!any(finite_r2)) {
    return(fallback)
  }

  best_r2 <- max(r2_vec[finite_r2])
  candidate_idx <- which(r2_vec >= (best_r2 - r2_tol))
  if (length(candidate_idx) == 0L) {
    candidate_idx <- which.max(r2_vec)
  }

  if (length(candidate_idx) > 1L) {
    candidate_labels <- names(r2_vec)[candidate_idx]
    candidate_ranks <- vapply(
      candidate_labels,
      complexity_rank_value,
      integer(1)
    )
    best_idx <- candidate_idx[[which.min(candidate_ranks)]]
  } else {
    best_idx <- candidate_idx[[1L]]
  }

  if (!is.finite(r2_vec[[best_idx]]) || r2_vec[[best_idx]] < min_best_r2_req) {
    return(list(
      best_class = "unknown",
      best_r2 = NA_real_,
      all_r2 = r2_vec,
      slope_per_n = NA_real_
    ))
  }

  list(
    best_class = fit_results[[best_idx]]$label,
    best_r2 = r2_vec[[best_idx]],
    all_r2 = r2_vec,
    slope_per_n = slope_vec[[best_idx]]
  )
}
