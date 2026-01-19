CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_person AS SELECT
    candidate_person.candidate_sid,
    candidate_person.valid_from_date,
    candidate_person.first_name,
    candidate_person.middle_name,
    candidate_person.last_name,
    candidate_person.maiden_name,
    candidate_person.social_security_num,
    candidate_person.email_address,
    candidate_person.birth_date,
    candidate_person.driver_license_num,
    candidate_person.driver_license_state_code,
    candidate_person.valid_to_date,
    candidate_person.source_system_code,
    candidate_person.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_person;