create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_employee_detail_type_stg`
(
  employee_detail_type_code STRING,
  employee_detail_type_desc STRING
)
