/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_pay_summary_group AS SELECT
      a.pay_summary_group_code,
      a.lawson_company_num,
      a.pay_summary_group_desc,
      a.pay_summary_abbreviation_desc,
      a.overtime_eligibility_pay_ind,
      a.overtime_eligibility_hour_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_pay_summary_group AS a
  ;

