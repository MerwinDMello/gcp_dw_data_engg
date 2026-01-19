/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_personnel_pay_type AS SELECT
    ref_personnel_pay_type.personnel_pay_type_code,
    ref_personnel_pay_type.personnel_pay_type_desc,
    ref_personnel_pay_type.source_system_code,
    ref_personnel_pay_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_personnel_pay_type
;
