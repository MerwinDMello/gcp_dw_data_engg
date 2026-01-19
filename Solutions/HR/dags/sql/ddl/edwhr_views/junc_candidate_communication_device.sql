/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_candidate_communication_device AS SELECT
      a.communication_device_sid,
      a.candidate_sid,
      a.communication_device_type_code,
      a.valid_from_date,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_candidate_communication_device AS a
  ;

