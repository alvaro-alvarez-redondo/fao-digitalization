options(
  fao.run_general_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)

build_temp_test_paths <- function(root_name) {
  root_dir <- file.path(tempdir(), root_name)
  unlink(root_dir, recursive = TRUE, force = TRUE)

  return(root_dir)
}

reference_is_file_like_path <- function(path_value) {
  path_file_name <- fs::path_file(path_value)
  path_extension <- fs::path_ext(path_file_name)

  return(nzchar(path_extension))
}

reference_resolve_directories <- function(paths) {
  all_paths <- unlist(paths, recursive = TRUE, use.names = FALSE)

  all_directories <- all_paths |>
    purrr::map_chr(\(path_value) {
      if (reference_is_file_like_path(path_value)) {
        return(fs::path_dir(path_value))
      }

      return(path_value)
    }) |>
    unique() |>
    sort()

  audit_root_dir <- resolve_audit_root_dir(paths)

  if (is.character(audit_root_dir) && length(audit_root_dir) == 1) {
    normalized_audit_root <- fs::path_norm(audit_root_dir)
    all_directories <- all_directories[
      !vapply(
        all_directories,
        \(path_value) {
          normalized_path <- fs::path_norm(path_value)
          identical(normalized_path, normalized_audit_root) ||
            startsWith(
              normalized_path,
              paste0(normalized_audit_root, .Platform$file.sep)
            )
        },
        logical(1)
      )
    ]
  }

  return(all_directories)
}

testthat::test_that("resolve_audit_root_dir returns configured scalar or NULL", {
  testthat::expect_null(resolve_audit_root_dir(list(exports = list())))

  audit_root <- resolve_audit_root_dir(
    list(data = list(audit = list(audit_root_dir = file.path("tmp", "audit"))))
  )

  testthat::expect_identical(audit_root, file.path("tmp", "audit"))
})

testthat::test_that("create_required_directories handles generic path lists without audit structure", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_generic")
  paths <- list(
    exports = list(
      lists = file.path(base_dir, "lists"),
      workbook = file.path(base_dir, "reports", "summary.xlsx")
    )
  )

  created_directories <- create_required_directories(paths)

  testthat::expect_true(dir.exists(file.path(base_dir, "lists")))
  testthat::expect_true(dir.exists(file.path(base_dir, "reports")))
  testthat::expect_true(
    all(c(file.path(base_dir, "lists"), file.path(base_dir, "reports")) %in% created_directories)
  )
})

testthat::test_that("create_required_directories treats uppercase file extensions as files", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_uppercase_extension")
  paths <- list(
    exports = list(
      workbook = file.path(base_dir, "reports", "SUMMARY.XLSX")
    )
  )

  created_directories <- create_required_directories(paths)

  testthat::expect_true(dir.exists(file.path(base_dir, "reports")))
  testthat::expect_false(dir.exists(file.path(base_dir, "reports", "SUMMARY.XLSX")))
  testthat::expect_identical(created_directories, file.path(base_dir, "reports"))
})

testthat::test_that("is_file_like_path classifies hidden names and extensions deterministically", {
  testthat::expect_true(is_file_like_path(file.path("exports", "SUMMARY.XLSX")))
  testthat::expect_true(is_file_like_path(file.path("exports", "summary.xlsx")))
  testthat::expect_false(is_file_like_path(file.path("cache", ".hidden")))
  testthat::expect_false(is_file_like_path(file.path("exports", "reports")))
})

testthat::test_that("is_file_like_path handles edge-case path literals deterministically", {
  testthat::expect_true(is_file_like_path("archive.tar.gz"))
  testthat::expect_false(is_file_like_path("data.v1/reports"))
  testthat::expect_false(is_file_like_path("reports."))
  testthat::expect_false(is_file_like_path("v1.2/reports"))
  testthat::expect_true(is_file_like_path("exports/summary.xlsx"))
  testthat::expect_true(is_file_like_path("exports/SUMMARY.XLSX"))
  testthat::expect_false(is_file_like_path("cache/.hidden"))
  testthat::expect_true(is_file_like_path("C:/data/reports/file.xlsx"))
  testthat::expect_true(is_file_like_path("C:\\data\\reports\\file.xlsx"))
})

testthat::test_that("create_required_directories keeps hidden directory names intact", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_hidden")
  hidden_dir <- file.path(base_dir, ".cache")
  paths <- list(cache = hidden_dir)

  created_directories <- create_required_directories(paths)

  testthat::expect_true(dir.exists(hidden_dir))
  testthat::expect_identical(created_directories, hidden_dir)
})

testthat::test_that("create_required_directories excludes audit root tree when configured", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_audit")
  paths <- list(
    data = list(
      imports = list(
        raw = file.path(base_dir, "imports", "raw")
      ),
      audit = list(
        audit_root_dir = file.path(base_dir, "audit"),
        audit_file_path = file.path(base_dir, "audit", "dataset", "audit.xlsx"),
        raw_imports_mirror_dir = file.path(base_dir, "audit", "raw_imports_mirror")
      )
    )
  )

  created_directories <- create_required_directories(paths)

  testthat::expect_true(dir.exists(file.path(base_dir, "imports", "raw")))
  testthat::expect_false(dir.exists(file.path(base_dir, "audit")))
  testthat::expect_false(any(startsWith(created_directories, file.path(base_dir, "audit"))))
})

testthat::test_that("create_required_directories matches reference resolver for valid path inputs", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_reference_parity")
  paths <- list(
    exports = list(
      workbook = file.path(base_dir, "reports", "SUMMARY.XLSX"),
      lists = file.path(base_dir, "lists"),
      archive = file.path(base_dir, "archives", "archive.tar.gz")
    ),
    imports = list(
      nested = file.path(base_dir, "imports", "v1.2", "reports")
    )
  )

  expected_directories <- reference_resolve_directories(paths)
  created_directories <- create_required_directories(paths)

  testthat::expect_identical(created_directories, expected_directories)
})

testthat::test_that("create_required_directories preserves reference audit exclusion behavior", {
  base_dir <- build_temp_test_paths("fao_directory_contracts_reference_audit_parity")
  paths <- list(
    data = list(
      imports = list(
        raw = file.path(base_dir, "imports", "raw"),
        workbook = file.path(base_dir, "imports", "raw", "source.xlsx")
      ),
      audit = list(
        audit_root_dir = file.path(base_dir, "audit"),
        audit_file_path = file.path(base_dir, "audit", "dataset", "audit.xlsx"),
        raw_imports_mirror_dir = file.path(base_dir, "audit", "raw_imports_mirror")
      )
    )
  )

  expected_directories <- reference_resolve_directories(paths)
  created_directories <- create_required_directories(paths)

  testthat::expect_identical(created_directories, expected_directories)
  testthat::expect_false(any(startsWith(created_directories, file.path(base_dir, "audit"))))
})


testthat::test_that("ensure_directories_exist creates sorted deterministic directories", {
  base_dir <- build_temp_test_paths("fao_ensure_directories")
  directories <- c(
    file.path(base_dir, "z_dir"),
    file.path(base_dir, "a_dir"),
    file.path(base_dir, "a_dir")
  )

  created <- ensure_directories_exist(directories, recurse = TRUE)

  testthat::expect_identical(created, sort(unique(directories)))
  testthat::expect_true(all(vapply(created, dir.exists, logical(1))))
})

testthat::test_that("delete_directory_if_exists removes existing directory", {
  base_dir <- build_temp_test_paths("fao_delete_directories")
  ensure_directories_exist(base_dir, recurse = TRUE)

  deleted <- delete_directory_if_exists(base_dir)

  testthat::expect_true(deleted)
  testthat::expect_false(dir.exists(base_dir))
})
