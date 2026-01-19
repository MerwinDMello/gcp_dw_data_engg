/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_performance_potential AS SELECT
    ref_performance_potential.performance_potential_sid,
    ref_performance_potential.performance_potential_desc,
    ref_performance_potential.source_system_code,
    ref_performance_potential.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_potential
    ;