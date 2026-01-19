/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_onboarding_resource AS SELECT
      a.resource_screening_package_num,
      a.valid_from_date,
      a.candidate_sid,
      a.recruitment_requisition_sid,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_resource AS a
  ;

