/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate AS SELECT
      a.candidate_sid,
      a.valid_from_date,
      a.candidate_num,
      a.in_hiring_process_sw,
      a.internal_candidate_sw,
      a.referred_sw,
      a.last_modified_date_time,
      a.candidate_creation_date_time,
      a.residence_location_num,
      a.travel_preference_code,
      a.relocation_preference_code,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate AS a
  ;

