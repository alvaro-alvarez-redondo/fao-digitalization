# tests reorganization mapping

## new structure plan

- tests/testthat/r/
  - test_setup_context.r
  - test_run_pipeline.r
  - test_pipeline_runners_backward_compatibility.r
  - 0_general_pipeline/
    - test_00_dependencies.r
    - test_01_setup_todo.r
    - test_02_helpers.r
  - 1_import_pipeline/
    - test_10_file_io_todo.r
    - test_11_reading_contracts.r
    - test_12_transform_functional_behavior.r
    - test_13_validate_log_transform_validation.r
    - test_15_output_raw_data_contract.r
    - test_run_import_pipeline_edge_cases.r
  - 3_export_pipeline/
    - test_30_data_audit_validation.r
    - test_30_data_audit_modular.r
    - test_30_data_audit_exported_contracts.r
    - test_30_data_audit_coverage.r
    - test_31_32_export_helpers.r

## old location -> new location

- tests/testthat/setup/test-00-context.r -> tests/testthat/r/test_setup_context.r
- tests/testthat/helpers/test-07-run-pipeline.r -> tests/testthat/r/test_run_pipeline.r
- tests/testthat/helpers/test-08-backward-compatibility-runners.r -> tests/testthat/r/test_pipeline_runners_backward_compatibility.r
- tests/testthat/helpers/test-06-dependencies.r -> tests/testthat/r/0_general_pipeline/test_00_dependencies.r
- tests/testthat/helpers/test-04-helpers.r -> tests/testthat/r/0_general_pipeline/test_02_helpers.r
- tests/testthat/edge_cases/test-09-reading-contracts.r -> tests/testthat/r/1_import_pipeline/test_11_reading_contracts.r
- tests/testthat/transformations/test-02-functional-behavior.r -> tests/testthat/r/1_import_pipeline/test_12_transform_functional_behavior.r
- tests/testthat/transformations/test-08-transform-validation.r -> tests/testthat/r/1_import_pipeline/test_13_validate_log_transform_validation.r
- tests/testthat/data_integrity/test-01-raw-data-contract.r -> tests/testthat/r/1_import_pipeline/test_15_output_raw_data_contract.r
- tests/testthat/edge_cases/test-03-edge-cases.r -> tests/testthat/r/1_import_pipeline/test_run_import_pipeline_edge_cases.r
- tests/testthat/helpers/test-05-export-helpers.r -> tests/testthat/r/3_export_pipeline/test_31_32_export_helpers.r
- tests/testthat/data_integrity/test-06-fao-data-validation.r -> tests/testthat/r/3_export_pipeline/test_30_data_audit_validation.r
- tests/testthat/data_integrity/test-07-data-audit-modular.r -> tests/testthat/r/3_export_pipeline/test_30_data_audit_modular.r
- tests/testthat/data_integrity/test-09-audit-exported-contracts.r -> tests/testthat/r/3_export_pipeline/test_30_data_audit_exported_contracts.r
- tests/testthat/data_integrity/test-10-export-audit-coverage.r -> tests/testthat/r/3_export_pipeline/test_30_data_audit_coverage.r

## todo notes for untested scripts

- R/0-general_pipeline/01-setup.R: load_pipeline_config, create_required_directories, ensure_output_directories
- R/1-import_pipeline/10-file_io.R: extract_file_metadata, discover_files, discover_pipeline_files
