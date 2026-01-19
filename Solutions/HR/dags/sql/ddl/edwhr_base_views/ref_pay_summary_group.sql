create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_pay_summary_group`
AS SELECT
ref_pay_summary_group.pay_summary_group_code,
ref_pay_summary_group.lawson_company_num,
ref_pay_summary_group.pay_summary_group_desc,
ref_pay_summary_group.pay_summary_abbreviation_desc,
ref_pay_summary_group.overtime_eligibility_pay_ind,
ref_pay_summary_group.overtime_eligibility_hour_ind,
ref_pay_summary_group.source_system_code,
ref_pay_summary_group.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_pay_summary_group;