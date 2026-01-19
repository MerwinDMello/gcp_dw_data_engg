create table if not exists `{{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2`
(
  requisition_approval_type_code STRING NOT NULL,
  requisition_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  approval_start_date DATE NOT NULL,
  approver_employee_num INT64 NOT NULL,
  approval_end_date DATE NOT NULL,
  approval_desc STRING,
  approver_position_title_desc STRING,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL,
  active_dw_ind STRING NOT NULL
)
