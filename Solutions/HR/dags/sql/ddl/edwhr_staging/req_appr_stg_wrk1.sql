create table if not exists `{{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk1`
(
  requisition INT64,
  company INT64,
  approval_start_date DATE,
  approval_end_date DATE,
  approval_desc STRING,
  approver_position_title_desc STRING,
  approval_emp INT64,
  requsition_approval_type_code STRING,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  active_dw_ind STRING NOT NULL
)
