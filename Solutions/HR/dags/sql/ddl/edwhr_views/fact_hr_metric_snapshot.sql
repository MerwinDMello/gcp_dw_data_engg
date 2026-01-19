/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.fact_hr_metric_snapshot AS SELECT
      a.employee_sid,
      a.requisition_sid,
      a.position_sid,
      a.date_id,
      a.analytics_msr_sid,
      a.snapshot_date,
      a.dept_sid,
      a.job_class_sid,
      a.job_code_sid,
      a.location_code,
      a.coid,
      a.company_code,
      a.functional_dept_num,
      a.sub_functional_dept_num,
      a.auxiliary_status_sid,
      a.employee_status_sid,
      a.key_talent_id,
      a.integrated_lob_id,
      a.action_code,
      a.action_reason_text,
      a.lawson_company_num,
      a.process_level_code,
      a.work_schedule_code,
      a.recruiter_owner_user_sid,
      a.requisition_approval_date,
      a.employee_num,
      a.metric_numerator_qty,
      a.metric_denominator_qty,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

