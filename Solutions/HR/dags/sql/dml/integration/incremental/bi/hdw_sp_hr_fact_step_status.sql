--################################################################################################{{ params.param_pub_views_dataset_name }}
--#                                                                                   			 #
--# Target Table - {{ params.param_hr_core_dataset_name }}.Fact_Step_Status            											 #
--#                                                                                   			 #
--# CHANGE CONTROL:                                                                   			 #
--#                                                                                   			 #
--# DATE          Developer     Change Comment                                        																												#
--# 04/15/2022    J Huertas     Initial Version                                        																												    #
--# 01/03/2023    N Lamborn     Added sub_status_Desc                                                                       																																		#
--# 01/10/2023    N Lamborn     Added Case to set empty string Sub_Status_Desc to NULL 
--# 03/24/2023    N Lamborn     Added moved_by_text                                                                             																																	   #
--#  This will load {{ params.param_hr_core_dataset_name }}.Fact_Step_Status        																																					#
--###############################################################################################



--FIRST STMT
DECLARE QB_Stmt  string;


--A&C
DECLARE Start_Date DATETIME;
DECLARE End_Date DATETIME;
-- SELECT Current_Timestamp(0) INTO :Start_Date;

set start_date = current_datetime('US/Central');




BEGIN
CREATE TEMPORARY TABLE sub
  CLUSTER BY stepstatus_uid, wf_row_rank
  AS
    SELECT
        concat(s.submission_sid, cp.candidate_profile_sid) AS stepstatus_uid,
        row_number() OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)) AS wf_row_rank,
        cp.candidate_profile_sid,
        cp.candidate_profile_num AS taleo_submission_id,
        c.candidate_num AS taleo_candidate_id,
        s.submission_sid,
        rr.recruitment_requisition_num_text AS taleo_requisition_num,
        s.recruitment_requisition_sid,
        rr.lawson_requisition_num,
        s.requisition_num as ghr_requisition_num,
        r.requisition_sid,
        st.tracking_step_id,
        cp.creation_date,
        cp.creation_date_time,
        cp.completion_date,
        cp.completion_date_time,
        CASE
          WHEN upper(trim(step_short_name)) = 'BACKGR. CHECK' THEN 'Background Process'
          WHEN upper(trim(step_short_name)) = 'HM REVIEW' THEN 'Hiring Manager Review'
          WHEN upper(trim(step_short_name)) = 'NEW' THEN 'New Applicant'
          WHEN upper(trim(step_short_name)) = 'ONB NOTICE' THEN 'New Hire Notification'
          WHEN upper(trim(step_short_name)) = 'PRESCREEN' THEN 'Pre-Employment Screen'
          ELSE step_short_name
        END AS step_short_name,
        step.step_name,
        status.submission_status_name,
        CASE
          WHEN upper(trim(st.sub_status_desc)) = '' THEN NULL
          ELSE st.sub_status_desc
        END AS sub_status_desc,
        st.moved_by_text,
        coalesce(st.event_date_time, st.creation_date_time) AS wf_status_start,
        lead(coalesce(st.event_date_time, st.creation_date_time), 1) OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)) AS wf_status_end,
        CASE
          WHEN date_diff(lead(coalesce(st.event_date_time, st.creation_date_time), 1) OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)), coalesce(st.event_date_time, st.creation_date_time), DAY) <= 365 THEN CAST(timestamp_diff(lead(coalesce(st.event_date_time, st.creation_date_time), 1) OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)), coalesce(st.event_date_time, st.creation_date_time), HOUR) as NUMERIC) / 24
          WHEN date_diff(lead(coalesce(st.event_date_time, st.creation_date_time), 1) OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)), coalesce(st.event_date_time, st.creation_date_time), DAY) BETWEEN 365 AND 9999 THEN CAST(timestamp_diff(lead(coalesce(st.event_date_time, st.creation_date_time), 1) OVER (PARTITION BY cp.candidate_profile_sid ORDER BY coalesce(st.event_date_time, st.creation_date_time)), coalesce(st.event_date_time, st.creation_date_time), DAY) as NUMERIC)
          ELSE CAST(NULL as NUMERIC)
        END AS duration,
        --  less than a year
        --  between 365 and 9999(Max size of DAY(4) interval field)
        --  NULLs and range beyond -9999 and 9999
        st.step_reverted_ind,
        hrdr.company_code,
        hrdr.coid,
        st.source_system_code,
        hrdr.cost_center AS dept_num
      FROM
        {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON st.candidate_profile_sid = cp.candidate_profile_sid
         AND date(cp.valid_to_date) = '9999-12-31'
         AND cp.creation_date BETWEEN '2018-01-01' AND current_date('US/Central')
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON cp.candidate_sid = c.candidate_sid
         AND date(c.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON cp.candidate_profile_sid = s.candidate_profile_sid
         AND date(s.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT
              recr.recruitment_requisition_sid,
              recr.lawson_requisition_sid,
              recr.recruitment_requisition_num_text,
              recr.lawson_requisition_num,
              recr.requisition_num,
              recr.recruitment_job_sid,
              recr.hiring_manager_user_sid,
              recr.source_system_code,
              recr.approved_sw
            FROM
              {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s_0 ON recr.recruitment_requisition_sid = s_0.recruitment_requisition_sid
               AND date(s_0.valid_to_date) = '9999-12-31'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s_0.submission_sid = o.submission_sid
               AND date(o.valid_to_date) = '9999-12-31'
            WHERE date(recr.valid_to_date) = '9999-12-31'
             AND recr.lawson_requisition_sid IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY recr.recruitment_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, recr.approved_sw DESC, o.sequence_num DESC) = 1
        ) AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON rr.lawson_requisition_sid = r.requisition_sid
         AND date(r.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS step ON st.tracking_step_id = step.step_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status AS sts ON st.submission_tracking_sid = sts.submission_tracking_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS status ON sts.submission_status_id = status.submission_status_id
         AND status.submission_status_name IS NOT NULL
         AND upper(trim(status.submission_status_name)) <> 'DRAFT'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
         AND date(rd.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
         AND date(rp.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
         AND date(jp.valid_to_date) = '9999-12-31'
         AND (jp.eff_to_date) = '9999-12-31'
         AND upper(trim(jp.active_dw_ind)) = 'Y'
         AND jp.account_unit_num IS NOT NULL
         AND jp.gl_company_num IS NOT NULL
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
         AND date(d.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT DISTINCT
              hrmet.coid,
              hrmet.company_code,
              hrmet.process_level_code,
              hrmet.lawson_company_num,
              hrmet.dept_sid,
              dept.dept_code,
              xwalk.dept_num AS cost_center,
              xwalk.gl_company_num,
              xwalk.account_unit_num
            FROM
              (
                SELECT DISTINCT
                    fact_hr_metric.coid,
                    fact_hr_metric.company_code,
                    fact_hr_metric.process_level_code,
                    fact_hr_metric.lawson_company_num,
                    fact_hr_metric.dept_sid
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric
              ) AS hrmet
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
               AND date(dept.valid_to_date) = '9999-12-31'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
               AND hrmet.process_level_code = xwalk.process_level_code
               AND dept.dept_code = xwalk.account_unit_num
               AND date(xwalk.valid_to_date) = '9999-12-31'
        ) AS hrdr ON r.process_level_code = hrdr.process_level_code
         AND d.dept_code = hrdr.dept_code
         AND r.lawson_company_num = hrdr.lawson_company_num
         AND jp.account_unit_num = hrdr.account_unit_num
         AND jp.gl_company_num = hrdr.gl_company_num
      WHERE date(st.valid_to_date) = '9999-12-31'
       AND st.tracking_step_id IS NOT NULL
       AND status.submission_status_name IS NOT NULL
       AND upper(trim(status.submission_status_name)) <> 'DRAFT'
       AND concat(s.submission_sid, cp.candidate_profile_sid) IS NOT NULL
       AND cp.creation_date >= DATE(current_date('US/Central') - INTERVAL 3 YEAR)
;
--  to pull only data from 3 year back



-- Delete prior data
TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.fact_step_status;



 -- INSERT data to core
INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_step_status (
  step_status_sid,
  step_seq_num,
  candidate_profile_sid,
  candidate_profile_num,
  candidate_num,
  submission_sid,
  recruitment_requisition_num_text,
  recruitment_requisition_sid,
  lawson_requisition_sid,
  ghr_requisition_num,
  requisition_sid,
  tracking_step_id,
  creation_date,
  creation_date_time,
  completion_date,
  completion_date_time,
  step_name,
  step_short_name,
  submission_status_name,
  sla_compliance_status_text,
  sub_status_desc,
  moved_by_text,
  step_status_start_date_time,
  step_status_end_date_time,
  company_code,
  coid,
  dept_num,
  duration_days_cnt,
  step_reverted_ind,
  non_working_day_cnt,
  source_system_code,
  dw_last_update_date_time)
  SELECT
      cast(stepstatus_uid as numeric) AS step_status_sid,
      wf_row_rank AS step_seq_num,
      candidate_profile_sid,
      taleo_submission_id AS candidate_profile_num,
      taleo_candidate_id AS candidate_num,
      submission_sid,
      taleo_requisition_num AS recruitment_requisition_num_text,
      recruitment_requisition_sid,
      lawson_requisition_num,
      ghr_requisition_num,
      requisition_sid,
      tracking_step_id,
      creation_date,
      creation_date_time,
      completion_date,
      completion_date_time,
      step_name,
      step_short_name,
      submission_status_name,
      max(CASE
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
         AND duration < 12 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
         AND duration >= 12
         AND duration < 15 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
         AND duration >= 15 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
         AND duration < 2 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
         AND duration >= 2
         AND duration < 3 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
         AND duration >= 3 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
         AND duration < 3 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
         AND duration >= 3
         AND duration < 4 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
         AND duration >= 4 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
         AND duration < 3 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
         AND duration >= 3
         AND duration < 4 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
         AND duration >= 4 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
         AND duration < 2 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
         AND duration >= 2
         AND duration < 3 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
         AND duration >= 3 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
         AND duration < 1 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
         AND duration >= 1
         AND duration < 2 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
         AND duration >= 2 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
         AND duration < 1 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
         AND duration >= 1
         AND duration < 2 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
         AND duration >= 2 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
         AND duration < 1 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
         AND duration >= 1
         AND duration < 2 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
         AND duration >= 2 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
         AND duration < 2 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
         AND duration >= 2
         AND duration < 3 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
         AND duration >= 3 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
         AND duration < 6 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
         AND duration >= 6
         AND duration < 8 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
         AND duration >= 8 THEN 'Breached'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
         AND duration < 1 THEN 'Healthy'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
         AND duration >= 1
         AND duration < 2 THEN 'At Risk'
        WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
         AND duration >= 2 THEN 'Breached'
        ELSE 'N/A'
      END) AS sla_compliance_status_text,
      -- HR Review
      -- HM Review
      -- Interview
      -- Offer
      sub_status_desc,
      moved_by_text,
      wf_status_start AS step_status_start_date_time,
      wf_status_end AS step_status_end_date_time,
      company_code,
      coid,
      dept_num,
      duration AS duration_days_cnt,
      step_reverted_ind,
      sum(CASE
        WHEN (lud.hca_holiday_flag) ='1' THEN 1
        WHEN (lud.week_end_flag) = 1 THEN 1
        ELSE 0
      END) AS non_working_day_cnt,
      source_system_code,
      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
    FROM
      sub
      left outer join unnest(generate_date_array(date(wf_status_start), date(wf_status_end))) date
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = date
       AND ((lud.hca_holiday_flag) = '1'
       OR (lud.week_end_flag) = 1)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, upper(CASE
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
       AND duration < 12 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
       AND duration >= 12
       AND duration < 15 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWUNDER CONSIDERATION'
       AND duration >= 15 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
       AND duration < 2 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
       AND duration >= 2
       AND duration < 3 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWTO BE EVALUATED'
       AND duration >= 3 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
       AND duration < 3 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
       AND duration >= 3
       AND duration < 4 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN REQUESTED'
       AND duration >= 4 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
       AND duration < 3 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
       AND duration >= 3
       AND duration < 4 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWVIDEO/VOICE SCREEN COMPLETED'
       AND duration >= 4 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
       AND duration < 2 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
       AND duration >= 2
       AND duration < 3 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWSELF SCHEDULE PHONE SCREEN REQUESTED'
       AND duration >= 3 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
       AND duration < 1 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
       AND duration >= 1
       AND duration < 2 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL OBTAINED'
       AND duration >= 2 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
       AND duration < 1 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
       AND duration >= 1
       AND duration < 2 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NOT OBTAINED'
       AND duration >= 2 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
       AND duration < 1 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
       AND duration >= 1
       AND duration < 2 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HR REVIEWHRSM TRANSFER APPROVAL NO RESPONSE'
       AND duration >= 2 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
       AND duration < 2 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
       AND duration >= 2
       AND duration < 3 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'HIRING MANAGER REVIEWTO BE REVIEWED'
       AND duration >= 3 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
       AND duration < 6 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
       AND duration >= 6
       AND duration < 8 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'INTERVIEWTO BE SCHEDULED'
       AND duration >= 8 THEN 'Breached'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
       AND duration < 1 THEN 'Healthy'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
       AND duration >= 1
       AND duration < 2 THEN 'At Risk'
      WHEN upper(trim(concat(step_short_name, submission_status_name))) = 'OFFEREXTENDED'
       AND duration >= 2 THEN 'Breached'
      ELSE 'N/A'
    END), 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32
;

END;
