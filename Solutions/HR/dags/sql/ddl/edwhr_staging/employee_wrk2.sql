create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_wrk2`
(
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  employee_num INT64 NOT NULL,
  hire_date DATE,
  primary_facility_ind STRING
)
