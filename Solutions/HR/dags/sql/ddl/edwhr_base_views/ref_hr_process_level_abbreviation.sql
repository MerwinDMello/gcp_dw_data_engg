/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_hr_process_level_abbreviation AS SELECT
    ref_hr_process_level_abbreviation.process_level_code,
    ref_hr_process_level_abbreviation.division_abbreviation_code,
    ref_hr_process_level_abbreviation.facility_abbreviation_code,
    ref_hr_process_level_abbreviation.source_system_code,
    ref_hr_process_level_abbreviation.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_hr_process_level_abbreviation
;