/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_onboarding_event AS SELECT
      a.candidate_onboarding_event_sid,
      a.valid_from_date,
      a.event_type_id,
      a.recruitment_requisition_num_text,
      a.valid_to_date,
      a.completed_date,
      a.candidate_sid,
      a.resource_screening_package_num,
      a.sequence_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event AS a
  ;

