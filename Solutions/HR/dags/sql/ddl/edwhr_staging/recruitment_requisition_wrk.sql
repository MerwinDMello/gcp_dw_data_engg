CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk
(
  file_date DATE,
  recruitment_requisition_sid INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  requisition_num INT64,
  lawson_requisition_sid INT64,
  lawson_requisition_num INT64,
  hiring_manager_user_sid INT64,
  recruitment_requisition_num_text STRING,
  process_level_code STRING NOT NULL,
  approved_sw INT64,
  target_start_date DATE,
  required_asset_num INT64,
  required_asset_sw INT64,
  workflow_id INT64,
  recruitment_job_sid INT64,
  job_template_sid INT64,
  requisition_new_graduate_flag STRING,
  lawson_company_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;
