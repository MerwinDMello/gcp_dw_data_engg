/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_division_pfte AS SELECT
      a.division_name,
      a.period_end_date,
      a.division_abbreviation_code,
      a.pfte_value_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_division_pfte AS a
  ;

