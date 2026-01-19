/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_requisition AS SELECT
      a.recruitment_requisition_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.requisition_num,
      a.lawson_requisition_sid,
      a.lawson_requisition_num,
      a.hiring_manager_user_sid,
      a.recruitment_requisition_num_text,
      a.process_level_code,
      a.approved_sw,
      a.target_start_date,
      a.required_asset_num,
      a.required_asset_sw,
      a.workflow_id,
      a.recruitment_job_sid,
      a.job_template_sid,
      a.requisition_new_graduate_flag,
      a.lawson_company_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS a
  ;

