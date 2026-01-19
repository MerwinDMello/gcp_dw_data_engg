CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_recruitment_location
AS SELECT
    ref_recruitment_location.location_num,
    ref_recruitment_location.level_num,
    ref_recruitment_location.location_name,
    ref_recruitment_location.location_code_text,
    ref_recruitment_location.work_location_code_text,
    ref_recruitment_location.addr_sid,
    ref_recruitment_location.source_system_code,
    ref_recruitment_location.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_recruitment_location;