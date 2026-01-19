/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_requisition_comment AS SELECT
      a.employee_sid,
      a.requisition_sid,
      a.applicant_type_id,
      a.comment_type_code,
      a.action_code,
      a.comment_line_num,
      a.sequence_num,
      a.hr_company_sid,
      a.valid_from_date,
      a.lawson_company_num,
      a.valid_to_date,
      a.comment_text,
      a.comment_date,
      a.print_code,
      a.process_level_code,
      a.requisition_num,
      a.employee_num,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_requisition_comment AS a
  ;

