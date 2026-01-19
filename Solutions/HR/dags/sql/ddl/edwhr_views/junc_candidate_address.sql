/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_candidate_address AS SELECT
      a.candidate_sid,
      a.valid_from_date,
      a.addr_sid,
      a.addr_type_code,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_candidate_address AS a
  ;

