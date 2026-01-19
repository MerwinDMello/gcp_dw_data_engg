CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.ref_exception`
(
  exception_code STRING NOT NULL,
  exception_desc STRING
)
CLUSTER BY exception_code;