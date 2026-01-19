CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factopenings AS SELECT
    -- --added 12-13-22
    -- --added 12-13-22
    -- --added 12-13-22
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    09/06/2019
    -- Update:  1/29/20 to relabel columns for consitancy and add Taleo Req #
    -- Update:  2/12/20 to add the First Posted Date
    -- Update:  2/27/20 Identify and Exclude drafts over 90 days
    -- Update:  5/1/20-  Replace ILOB and Key Talent Joins with Integrated IDs from Fact_HR_Metric
    -- Update:  09/30/20-  Modify posting date to inlcude status of 1003 for ATS
    -- Update:  08/27/2021- Added columns for Core/Flex logic HRGA-1032
    -- Update: 09/16/2021- Modifited join to recruitment job and to the Recruitment User tables to pull historic recruiter name based on date_ID. Renamed Status_Code.
    --  Update: 03/07/2022-  Modified the first posted date because ATS gets this from a different table.  Add coalesce
    -- Update 12/12/2022-  Pointed at the new table   EDWHR_BASE_VIEWS,Fact_Openings_Snapshot
    --  Description:  Openings
    -- Metrics:  # of Openings, Days open
    --  =============================================
    fact_opening_snapshot.period_end_date AS pe_date,
    fact_opening_snapshot.analytics_msr_sid,
    fact_opening_snapshot.date_id,
    fact_opening_snapshot.process_level_uid AS pl_uid,
    fact_opening_snapshot.workflow_code,
    fact_opening_snapshot.coid,
    fact_opening_snapshot.requisition_sid,
    fact_opening_snapshot.recruitment_requisition_sid,
    fact_opening_snapshot.recruitment_requisition_num_text AS taleo_requisition_num,
    fact_opening_snapshot.lawson_requisition_num,
    fact_opening_snapshot.ghr_requisition_num,
    fact_opening_snapshot.requisition_sid AS req_requisition_sid,
    fact_opening_snapshot.key_talent_id,
    CASE
      WHEN fact_opening_snapshot.status_code = '01' THEN 'FT'
      WHEN fact_opening_snapshot.status_code = '02' THEN 'PT'
      WHEN fact_opening_snapshot.status_code = '03' THEN 'PRN'
      WHEN fact_opening_snapshot.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    fact_opening_snapshot.position_sid,
    fact_opening_snapshot.position_key,
    fact_opening_snapshot.job_code_sid,
    fact_opening_snapshot.location_code,
    fact_opening_snapshot.functional_dept_num,
    fact_opening_snapshot.sub_functional_dept_num,
    fact_opening_snapshot.dept_num AS cost_center,
    fact_opening_snapshot.dept_sid,
    fact_opening_snapshot.dept_name,
    fact_opening_snapshot.dept_code,
    fact_opening_snapshot.process_level_code,
    fact_opening_snapshot.work_schedule_code,
    fact_opening_snapshot.job_schedule_desc AS job_schedule,
    CASE
      WHEN upper(fact_opening_snapshot.work_schedule_code) LIKE '1%' THEN 'Days'
      WHEN upper(fact_opening_snapshot.work_schedule_code) LIKE '2%' THEN 'Eves'
      WHEN upper(fact_opening_snapshot.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
      WHEN upper(fact_opening_snapshot.work_schedule_code) LIKE '3%' THEN 'Nights'
      WHEN upper(fact_opening_snapshot.work_schedule_code) LIKE 'X%' THEN 'Mixed'
      WHEN upper(fact_opening_snapshot.work_schedule_code) = 'VARY' THEN 'Mixed'
      WHEN upper(fact_opening_snapshot.work_schedule_code) = 'WFH' THEN 'WFH'
      ELSE 'Unknown'
    END AS shift_desc,
    fact_opening_snapshot.open_fte_percent AS fte_value,
    fact_opening_snapshot.requisition_approval_date,
    fact_opening_snapshot.requisition_open_day_cnt AS req_open_time,
    fact_opening_snapshot.final_status_code,
    fact_opening_snapshot.employee_num,
    fact_opening_snapshot.recruiter_owner_user_sid,
    fact_opening_snapshot.recruiter_name,
    fact_opening_snapshot.hiring_manager_user_sid,
    fact_opening_snapshot.hiring_manager_name,
    fact_opening_snapshot.removed_ind AS removed,
    fact_opening_snapshot.patient_care_position_ind AS pct,
    fact_opening_snapshot.integrated_lob_id,
    fact_opening_snapshot.prn_tier_text AS prn_tier,
    fact_opening_snapshot.workforce_category_text AS workforce_category,
    fact_opening_snapshot.first_posted_date AS first_posted,
    fact_opening_snapshot.metric_numerator_qty AS openings,
    fact_opening_snapshot.source_system_code,
    fact_opening_snapshot.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_opening_snapshot
;
