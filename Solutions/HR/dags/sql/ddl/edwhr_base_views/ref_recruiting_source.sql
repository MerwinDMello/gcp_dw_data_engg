/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_recruiting_source AS SELECT
    ref_recruiting_source.recruiting_source_code,
    ref_recruiting_source.recruiting_source_name,
    ref_recruiting_source.recruiting_source_type_desc,
    ref_recruiting_source.recruiting_availability_ind,
    ref_recruiting_source.source_system_code,
    ref_recruiting_source.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_recruiting_source
;