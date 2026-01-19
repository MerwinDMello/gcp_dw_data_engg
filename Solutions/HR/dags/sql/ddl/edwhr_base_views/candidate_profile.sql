create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate_profile`
AS SELECT
    candidate_profile.candidate_profile_sid,
    candidate_profile.valid_from_date,
    candidate_profile.candidate_sid,
    candidate_profile.profile_medium_id,
    candidate_profile.candidate_profile_num,
    candidate_profile.submission_date,
	candidate_profile.submission_date_time,
    candidate_profile.completion_date,
	candidate_profile.completion_date_time,
    candidate_profile.creation_date,
	candidate_profile.creation_date_time,
    candidate_profile.recruitment_source_id,
    candidate_profile.recruitment_source_auto_filled_sw,
    candidate_profile.valid_to_date,
    candidate_profile.requisition_num,
    candidate_profile.job_application_num,
    candidate_profile.candidate_num,
    candidate_profile.source_system_code,
    candidate_profile.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.candidate_profile;