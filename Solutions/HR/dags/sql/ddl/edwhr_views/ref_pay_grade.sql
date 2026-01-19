/***************************************************************************************
   S E C U R I T Y  V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_pay_grade AS SELECT
      ref_pay_grade.pay_grade_code,
      ref_pay_grade.pay_grade_desc,
      ref_pay_grade.source_system_code,
      ref_pay_grade.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_pay_grade
  ;

