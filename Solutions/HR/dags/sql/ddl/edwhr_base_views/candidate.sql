create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate`
AS SELECT
    candidate.candidate_sid,
    candidate.valid_from_date,
    candidate.candidate_num,
    candidate.in_hiring_process_sw,
    candidate.internal_candidate_sw,
    candidate.referred_sw,
    candidate.last_modified_date_time,
    candidate.candidate_creation_date_time,
    candidate.residence_location_num,
    candidate.travel_preference_code,
    candidate.relocation_preference_code,
    candidate.valid_to_date,
    candidate.source_system_code,
    candidate.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.candidate;