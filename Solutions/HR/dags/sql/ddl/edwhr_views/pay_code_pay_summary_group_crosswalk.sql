/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.pay_code_pay_summary_group_crosswalk AS SELECT
      a.clock_library_code,
      a.kronos_pay_code,
      a.kronos_pay_code_desc,
      a.lawson_pay_summary_group_code,
      a.lawson_pay_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.pay_code_pay_summary_group_crosswalk AS a
  ;

