/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_profile AS SELECT
      a.candidate_profile_sid,
      a.valid_from_date,
      a.candidate_sid,
      a.profile_medium_id,
      a.candidate_profile_num,
      a.submission_date,
	  a.submission_date_time,
      a.completion_date,
	  a.completion_date_time,
      a.creation_date,
	  a.creation_date_time,
      a.recruitment_source_id,
      a.recruitment_source_auto_filled_sw,
      a.valid_to_date,
      a.requisition_num,
      a.job_application_num,
      a.candidate_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS a
  ;

