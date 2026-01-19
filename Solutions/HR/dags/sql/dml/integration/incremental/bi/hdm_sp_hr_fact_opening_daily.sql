
BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = current_datetime('US/Central');

TRUNCATE TABLE {{params.param_hr_core_dataset_name}}.fact_opening_daily;

BEGIN TRANSACTION;
INSERT INTO {{params.param_hr_core_dataset_name}}.fact_opening_daily (employee_sid, analytics_msr_sid, date_id, process_level_uid, workflow_code, coid, company_code, requisition_sid, recruitment_requisition_sid, recruitment_requisition_num_text, lawson_requisition_num, ghr_requisition_num, status_desc, key_talent_id, status_code, position_sid, position_key, job_code_sid, location_code, functional_dept_num, sub_functional_dept_num, dept_num, dept_sid, dept_name, dept_code, process_level_code, work_schedule_code, job_schedule_desc, open_fte_percent, requisition_approval_date, requisition_open_day_cnt, final_status_code, employee_num, recruiter_owner_user_sid, recruiter_name, hiring_manager_user_sid, hiring_manager_name, removed_ind, patient_care_position_ind, integrated_lob_id, prn_tier_text, workforce_category_text, first_posted_date, metric_numerator_qty, source_system_code, dw_last_update_date_time)
WITH sub AS (
SELECT
      hrmet.employee_sid,
      hrmet.analytics_msr_sid,
      hrmet.position_sid,
      hrmet.requisition_sid,
      hrmet.date_id,
      concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS process_level_uid,
      rw.workflow_code,
      hrmet.coid,
      hrmet.company_code,
      rr.recruitment_requisition_sid,
      rr.recruitment_requisition_num_text,
      rr.lawson_requisition_num,
      CASE
        WHEN upper(trim(rr.source_system_code)) = 'B' THEN rr.requisition_num
        ELSE NULL
      END AS ghr_requisition_num,
      req_his.status_desc,
      hrmet.key_talent_id,
      stas.status_code,
      concat(hrmet.position_sid, jobpos.eff_to_date)  AS position_key,
      hrmet.job_code_sid,
      hrmet.location_code,
      hrmet.functional_dept_num,
      hrmet.sub_functional_dept_num,
      hrdr.cost_center AS dept_num,
      hrmet.dept_sid,
      d.dept_name,
      rd.dept_code,
      hrmet.process_level_code,
      hrmet.work_schedule_code,
      rjs.job_schedule_desc,
      r.open_fte_percent,
      hrmet.requisition_approval_date,
     DATE_DIFF(hrmet.date_id,hrmet.requisition_approval_date,DAY)  AS requisition_open_day_cnt,
      rs.status_code AS final_status_code,
      recuser.employee_num,
      hrmet.recruiter_owner_user_sid,
      concat(trim(recuser.last_name), ', ', recuser.first_name) AS recruiter_name,
      rr.hiring_manager_user_sid,
      concat(trim(recuser2.last_name), ', ', recuser2.first_name) AS hiring_manager_name,
      CASE
        WHEN reqrs.requisition_status_id = 1
         AND hrmet.requisition_approval_date < date_add(last_day(date_add(current_date('US/Central'), interval -3 MONTH)), interval 1 DAY) THEN 'Deleted'
        ELSE 'No'
      END AS removed_ind,
      CASE
        WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
        ELSE 'No'
      END AS patient_care_position_ind,
      hrmet.integrated_lob_id,
      CASE
        WHEN (upper(trim(jobpos.position_code_desc)) LIKE '%PRN I'
         OR upper(trim(jobpos.position_code_desc)) LIKE '%PRN')
         AND upper(trim(jcl.job_class_code)) = '103' THEN 'PRN Tier 1'
        WHEN upper(trim(jobpos.position_code_desc)) LIKE '%PRN II'
         AND upper(trim(jcl.job_class_code)) = '103' THEN 'PRN Tier 2'
        WHEN upper(trim(jobpos.position_code_desc)) LIKE '%PRN III'
         AND upper(trim(jcl.job_class_code)) = '103' THEN 'PRN Tier 3'
        ELSE CAST(NULL as STRING)
      END AS prn_tier_text,
      CASE
        WHEN upper(trim(stas.status_code)) IN(
          '01', '02'
        )
         OR (upper(trim(jobpos.position_code_desc)) LIKE '%PRN II'
         OR upper(trim(jobpos.position_code_desc)) LIKE '%PRN III') THEN 'Core'
        ELSE 'Flex'
      END AS workforce_category_text,
      coalesce(DATE(postdate.creation_date_time), postdate2.posting_date) AS first_posted_date,
      hrmet.metric_numerator_qty,
      hrmet.source_system_code,
      hrmet.dw_last_update_date_time
    FROM
      {{params.param_hr_base_views_dataset_name}}.fact_hr_metric AS hrmet
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.department AS d ON hrmet.dept_sid = d.dept_sid
       AND d.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.hr_dept_rollup AS hrdr ON hrmet.process_level_code = hrdr.process_level_code
       AND hrmet.coid = hrdr.coid
       AND hrmet.dept_sid = hrdr.dept_sid
       AND hrmet.lawson_company_num = hrdr.lawson_company_num
      LEFT OUTER JOIN {{params.param_pub_views_dataset_name}}.fact_facility AS ff ON hrmet.coid = ff.coid
       AND hrmet.company_code = ff.company_code
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.job_class AS jcl ON hrmet.job_class_sid = jcl.job_class_sid
       AND jcl.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.job_code AS jc ON hrmet.job_code_sid = jc.job_code_sid
       AND jc.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
       AND jobpos.valid_to_date = DATETIME("9999-12-31 23:59:59")
       AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_location AS rl ON hrmet.location_code = rl.location_code
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.requisition_status AS rs ON hrmet.requisition_sid = rs.requisition_sid
       AND rs.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.requisition AS r ON hrmet.requisition_sid = r.requisition_sid
       AND r.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
       AND rd.valid_to_date = DATETIME("9999-12-31 23:59:59")
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
            o.accept_date,
            o.offer_sid,
            recr.process_level_code,
            ros.offer_status_desc,
            recr.approved_sw,
            s.submission_sid,
            recr.workflow_id
          FROM
            {{params.param_hr_base_views_dataset_name}}.recruitment_requisition AS recr
            LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
             AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
            LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.offer AS o ON s.submission_sid = o.submission_sid
             AND o.valid_to_date = DATETIME("9999-12-31 23:59:59")
            LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.offer_status AS os ON os.offer_sid = o.offer_sid
             AND os.valid_to_date = DATETIME("9999-12-31 23:59:59")
            LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
          WHERE recr.valid_to_date = DATETIME("9999-12-31 23:59:59")
          QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
      ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
      LEFT OUTER JOIN -- Removed o.Last_Modified_Date as specified in HDM-2096
      (
        SELECT
            subq2.*
          FROM
            (
              SELECT
                  d_0.date_id,
                  subq.recruitment_requisition_sid,
                  subq.creation_date_time,
                  subq.req_status_end,
                  subq.requisition_status_id,
                  subq.status_desc,
                  subq.source_system_code,
                  row_number() OVER (PARTITION BY subq.recruitment_requisition_sid, d_0.date_id ORDER BY subq.creation_date_time) AS row_num
                FROM
                  (
                    SELECT
                        rrh.recruitment_requisition_sid,
                        rrh.creation_date_time,
                        lead(rrh.creation_date_time, 1) OVER (PARTITION BY rrh.recruitment_requisition_sid ORDER BY rrh.creation_date_time) AS req_status_end,
                        rrh.requisition_status_id,
                        refstatus.status_desc,
                        rrh.source_system_code
                      FROM
                        {{params.param_hr_base_views_dataset_name}}.recruitment_requisition_history AS rrh
                        LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_requisition_status AS refstatus ON rrh.requisition_status_id = refstatus.requisition_status_id
                         AND rrh.source_system_code = refstatus.source_system_code
                      WHERE valid_to_date = DATETIME("9999-12-31 23:59:59")
                  ) AS subq
                  INNER JOIN {{params.param_pub_views_dataset_name}}.lu_date AS d_0 ON d_0.date_id >= DATE(subq.creation_date_time)
                   AND d_0.date_id <= CAST(coalesce(subq.req_status_end, current_ts) as date)
            ) AS subq2
          WHERE subq2.row_num = 1
      ) AS req_his ON rr.recruitment_requisition_sid = req_his.recruitment_requisition_sid
       AND rr.source_system_code = req_his.source_system_code
       AND hrmet.date_id = req_his.date_id
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_workflow AS rw ON rr.workflow_id = rw.workflow_id
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.recruitment_requisition_status AS reqrs ON rr.recruitment_requisition_sid = reqrs.recruitment_requisition_sid
       AND reqrs.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN (
        SELECT
            recruitment_requisition_history.creation_date_time,
            recruitment_requisition_history.source_system_code,
            recruitment_requisition_history.recruitment_requisition_sid,
            rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time) AS firstsourcing
          FROM
            {{params.param_hr_base_views_dataset_name}}.recruitment_requisition_history
          WHERE recruitment_requisition_history.requisition_status_id = 13
           AND upper(trim(recruitment_requisition_history.source_system_code)) = 'T'
      ) AS postdate ON rr.recruitment_requisition_sid = postdate.recruitment_requisition_sid
       AND rr.source_system_code = postdate.source_system_code
       AND postdate.firstsourcing = 1
      LEFT OUTER JOIN (
        SELECT DISTINCT
            req.recruitment_requisition_sid,
            req.source_system_code,
            req.posting_date,
            rank() OVER (PARTITION BY req.recruitment_requisition_sid ORDER BY req.posting_date) AS firstposted
          FROM
            {{params.param_hr_base_views_dataset_name}}.sourcing_request AS req
          WHERE req.valid_to_date = DATETIME("9999-12-31 23:59:59")
           AND upper(trim(req.source_system_code)) = 'B'
           AND req.posting_date IS NOT NULL
      ) AS postdate2 ON rr.recruitment_requisition_sid = postdate2.recruitment_requisition_sid
       AND rr.source_system_code = postdate2.source_system_code
       AND postdate2.firstposted = 1
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.recruitment_job AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
       AND hrmet.date_id BETWEEN DATE(rj.valid_from_date) AND DATE(rj.valid_to_date)
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_job_schedule AS rjs ON rj.job_schedule_id = rjs.job_schedule_id
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
       AND hrmet.date_id BETWEEN DATE(recuser.valid_from_date) AND DATE(recuser.valid_to_date)
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.recruitment_user AS recuser2 ON rr.hiring_manager_user_sid = recuser2.recruitment_user_sid
       AND hrmet.date_id BETWEEN DATE(recuser2.valid_from_date) AND DATE(recuser2.valid_to_date)
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.requisition_position AS reqpos ON r.requisition_sid = reqpos.requisition_sid
       AND reqpos.valid_to_date = DATETIME("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
      LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.status AS stas ON r.application_status_sid = stas.status_sid
       AND stas.valid_to_date = DATETIME("9999-12-31 23:59:59")
    WHERE hrmet.analytics_msr_sid = 80600
     AND hrmet.date_id > date_add(current_date('US/Central'), interval -37 MONTH)
     AND CASE
      WHEN reqrs.requisition_status_id = 1
       AND hrmet.requisition_approval_date < date_add(last_day(date_add(current_date('US/Central'), interval -3 MONTH)), interval 1 DAY) THEN 'Deleted'
      ELSE 'No'
    END = 'No'
)

    SELECT
        sub.employee_sid,
        sub.analytics_msr_sid,
        sub.date_id,
        sub.process_level_uid,
        sub.workflow_code,
        sub.coid,
        sub.company_code,
        sub.requisition_sid,
        sub.recruitment_requisition_sid,
        sub.recruitment_requisition_num_text,
        sub.lawson_requisition_num,
        sub.ghr_requisition_num,
        sub.status_desc,
        sub.key_talent_id,
        sub.status_code,
        sub.position_sid,
        sub.position_key,
        sub.job_code_sid,
        sub.location_code,
        sub.functional_dept_num,
        sub.sub_functional_dept_num,
        sub.dept_num,
        sub.dept_sid,
        sub.dept_name,
        sub.dept_code,
        sub.process_level_code,
        sub.work_schedule_code,
        sub.job_schedule_desc,
        sub.open_fte_percent,
        sub.requisition_approval_date,
        sub.requisition_open_day_cnt,
        sub.final_status_code,
        sub.employee_num,
        sub.recruiter_owner_user_sid,
        sub.recruiter_name,
        sub.hiring_manager_user_sid, 
        sub.hiring_manager_name,
        sub.removed_ind,
        sub.patient_care_position_ind,
        sub.integrated_lob_id,
        sub.prn_tier_text,
        sub.workforce_category_text,
        sub.first_posted_date,
        sub.metric_numerator_qty,
        sub.source_system_code,
        sub.dw_last_update_date_time
      FROM
        sub
  ;


/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Employee_SID ,Analytics_Msr_SID ,Position_SID ,Requisition_SID ,Date_Id 
  from  {{params.param_hr_core_dataset_name}}.fact_opening_daily
  group by Employee_SID ,Analytics_Msr_SID ,Position_SID ,Requisition_SID ,Date_Id 
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table: edwhr_copy.fact_opening_daily');
ELSE  
  COMMIT  TRANSACTION;
END IF;


END;

