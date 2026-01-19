
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ta_positions AS SELECT
      v.date_id,
      last_day(DATE(v.date_id)) AS pe_date,
      'Y' AS vacancy,
      o.opening,
      f.filled,
      v.metric_numerator_qty AS days_vacant,
      o.days_open,
      f.fill_count,
      v.requisition_sid,
      v.requisition_approval_date,
      v.lawson_company_num,
      v.coid,
      v.process_level_code,
      v.dept_sid,
      v.functional_dept_num,
      v.sub_functional_dept_num,
      v.location_code,
      v.work_schedule_code,
      v.job_class_sid,
      v.job_code_sid,
      v.position_sid,
      v.key_talent_id,
      v.integrated_lob_id,
      coalesce(f.recruiter_owner_user_sid, o.recruiter_owner_user_sid) AS recruiter_owner_user_sid
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS v
      LEFT OUTER JOIN (
        SELECT
            o_0.date_id,
            'Y' AS opening,
            o_0.requisition_sid,
            o_0.requisition_approval_date,
            o_0.lawson_company_num,
            o_0.coid,
            o_0.process_level_code,
            o_0.dept_sid,
            o_0.functional_dept_num,
            o_0.sub_functional_dept_num,
            o_0.location_code,
            o_0.work_schedule_code,
            o_0.job_class_sid,
            o_0.job_code_sid,
            o_0.position_sid,
            o_0.key_talent_id,
            o_0.integrated_lob_id,
            o_0.recruiter_owner_user_sid,
            o_0.metric_numerator_qty AS days_open
          FROM
            {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS o_0
          WHERE o_0.analytics_msr_sid = 80600
      ) AS o ON v.requisition_sid = o.requisition_sid
       AND v.date_id = o.date_id
      LEFT OUTER JOIN -- and o.Requisition_SID = '2816896'
      (
        SELECT
            f_0.date_id,
            'Y' AS filled,
            f_0.requisition_sid,
            f_0.requisition_approval_date,
            f_0.lawson_company_num,
            f_0.coid,
            f_0.process_level_code,
            f_0.dept_sid,
            f_0.functional_dept_num,
            f_0.sub_functional_dept_num,
            f_0.location_code,
            f_0.work_schedule_code,
            f_0.job_class_sid,
            f_0.job_code_sid,
            f_0.position_sid,
            f_0.key_talent_id,
            f_0.integrated_lob_id,
            f_0.recruiter_owner_user_sid,
            f_0.metric_numerator_qty AS fill_count
          FROM
            {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS f_0
          WHERE f_0.analytics_msr_sid = 80500
      ) AS f ON v.requisition_sid = f.requisition_sid
       AND v.date_id = f.date_id
    WHERE v.analytics_msr_sid = 80400
  ;

-- and v.Requisition_SID = '2816896'
-- and v.requisition_sid = '2891878';;
