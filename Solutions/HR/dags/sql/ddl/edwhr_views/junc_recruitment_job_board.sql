/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_recruitment_job_board AS SELECT
      a.recruitment_job_sid,
      a.job_board_id,
      a.valid_from_date,
      a.posting_board_type_id,
      a.posting_status_id,
      a.valid_to_date,
      a.posting_date,
      a.unposting_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_recruitment_job_board AS a
  ;

