/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.sourcing_request AS SELECT
      a.sourcing_request_sid,
      a.valid_from_date,
      a.recruitment_requisition_sid,
      a.job_board_id,
      a.source_request_status_id,
      a.job_board_type_id,
      a.valid_to_date,
      a.posting_date,
      a.unposting_date,
      a.creation_date,
      a.requisition_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.sourcing_request AS a
  ;

