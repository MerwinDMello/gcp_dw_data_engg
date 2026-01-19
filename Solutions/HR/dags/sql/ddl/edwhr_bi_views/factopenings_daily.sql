CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factopenings_daily AS SELECT
    -- - added 12-13-22
    -- - added 12-13-22 to reflect status of requisition in recruitment system on given day
    -- -- added 12-13-22
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    09/06/2019
    -- Update:  1/29/20 to relabel columns for consitancy and add Taleo Req #
    -- Update:  2/12/20 to add the First Posted Date
    -- Update:  2/27/20 Identify and Exclude drafts over 90 days
    -- Update:  5/1/20-  Replace ILOB and Key Talent Joins with Integrated IDs from Fact_HR_Metric
    -- Update:  09/30/20-  Modify posting date to inlcude status of 1003 for ATS
    -- -Update 11/13/2020 to create view for use in daily model- edit PE_Date, use Fact HR Metric rather than snapshot, change date filter
    -- -Updated 9/16/21 to modify recruiter user and recruitment job joins, rename Status_Code
    -- -Updated 5/25/22 to add DW_Last_Update_Date_Time for TA
    -- -Updated 12/12/22 to point it to the new table
    --  Description:  Openings Daily
    -- Metrics:  # of Openings, Days open
    --  =============================================
    fact_opening_daily.date_id AS pe_date,
    fact_opening_daily.analytics_msr_sid,
    fact_opening_daily.date_id,
    fact_opening_daily.process_level_uid AS pl_uid,
    fact_opening_daily.workflow_code,
    fact_opening_daily.coid,
    fact_opening_daily.requisition_sid,
    fact_opening_daily.recruitment_requisition_sid,
    fact_opening_daily.recruitment_requisition_num_text AS taleo_requisition_num,
    fact_opening_daily.lawson_requisition_num,
    fact_opening_daily.ghr_requisition_num,
    fact_opening_daily.requisition_sid AS req_requisition_sid,
    fact_opening_daily.key_talent_id,
    CASE
      WHEN fact_opening_daily.status_code = '01' THEN 'FT'
      WHEN fact_opening_daily.status_code = '02' THEN 'PT'
      WHEN fact_opening_daily.status_code = '03' THEN 'PRN'
      WHEN fact_opening_daily.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    fact_opening_daily.position_sid,
    fact_opening_daily.position_key,
    fact_opening_daily.job_code_sid,
    fact_opening_daily.location_code,
    fact_opening_daily.functional_dept_num,
    fact_opening_daily.sub_functional_dept_num,
    fact_opening_daily.dept_num AS cost_center,
    fact_opening_daily.dept_sid,
    fact_opening_daily.dept_name,
    fact_opening_daily.dept_code,
    fact_opening_daily.process_level_code,
    fact_opening_daily.work_schedule_code,
    fact_opening_daily.job_schedule_desc AS job_schedule,
    CASE
      WHEN upper(fact_opening_daily.work_schedule_code) LIKE '1%' THEN 'Days'
      WHEN upper(fact_opening_daily.work_schedule_code) LIKE '2%' THEN 'Eves'
      WHEN upper(fact_opening_daily.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
      WHEN upper(fact_opening_daily.work_schedule_code) LIKE '3%' THEN 'Nights'
      WHEN upper(fact_opening_daily.work_schedule_code) LIKE 'X%' THEN 'Mixed'
      WHEN upper(fact_opening_daily.work_schedule_code) = 'VARY' THEN 'Mixed'
      WHEN upper(fact_opening_daily.work_schedule_code) = 'WFH' THEN 'WFH'
      ELSE 'Unknown'
    END AS shift_desc,
    fact_opening_daily.open_fte_percent AS fte_value,
    fact_opening_daily.requisition_approval_date,
    fact_opening_daily.requisition_open_day_cnt AS req_open_time,
    fact_opening_daily.final_status_code,
    fact_opening_daily.status_desc AS recruit_req_status,
    fact_opening_daily.employee_num,
    fact_opening_daily.recruiter_owner_user_sid,
    fact_opening_daily.recruiter_name,
    fact_opening_daily.hiring_manager_user_sid,
    fact_opening_daily.hiring_manager_name,
    fact_opening_daily.removed_ind AS removed,
    fact_opening_daily.patient_care_position_ind AS pct,
    fact_opening_daily.integrated_lob_id,
    fact_opening_daily.prn_tier_text AS prn_tier,
    fact_opening_daily.workforce_category_text AS workforce_category,
    fact_opening_daily.first_posted_date AS first_posted,
    fact_opening_daily.metric_numerator_qty AS openings,
    fact_opening_daily.source_system_code,
    fact_opening_daily.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_opening_daily
;
