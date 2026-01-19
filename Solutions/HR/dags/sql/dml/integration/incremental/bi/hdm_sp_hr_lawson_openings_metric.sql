BEGIN
DECLARE DUP_COUNT INT64;

CREATE TEMPORARY TABLE ts AS (
    SELECT
      a.recruitment_requisition_sid,
      CASE
        WHEN UPPER(a.source_system_code) = 'T' THEN a.creation_date_time
        WHEN UPPER(a.source_system_code) = 'B' THEN ghrhist.creation_date_time
      END AS creation_date_time,
      coalesce(date_sub(DATE(b.creation_date_time), interval 1 DAY), DATE '9999-12-31') AS status_end_date,
      a.requisition_status_id,
      a.recruiter_owner_user_sid,
      a.row_rank1,
      b.row_rank2
    FROM
      (
        SELECT
            aa.recruitment_requisition_sid,
            aa.creation_date_time,
            aa.requisition_status_id,
            aa.recruiter_owner_user_sid,
            aa.valid_to_date,
            aa.closed_date_time,
            aa.source_system_code,
            row_number() OVER (PARTITION BY aa.recruitment_requisition_sid ORDER BY aa.creation_date_time) AS row_rank1
          FROM
            (
              SELECT
                  rr.recruitment_requisition_sid,
                  rr.creation_date_time,
                  rr.requisition_status_id,
                  rr.recruiter_owner_user_sid,
                  rr.valid_to_date,
                  rr.closed_date_time,
                  rr.source_system_code
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history AS rr
                WHERE rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
                QUALIFY row_number() OVER (PARTITION BY rr.recruitment_requisition_sid, rr.creation_date_time ORDER BY rr.creation_date_time DESC) = 1
            ) AS aa
      ) AS a
      LEFT OUTER JOIN (
        SELECT
            bb.recruitment_requisition_sid,
            bb.creation_date_time,
            bb.requisition_status_id,
            bb.recruiter_owner_user_sid,
            bb.valid_to_date,
            bb.closed_date_time,
            row_number() OVER (PARTITION BY bb.recruitment_requisition_sid ORDER BY bb.creation_date_time) AS row_rank2
          FROM
            (
              SELECT
                  rrh.recruitment_requisition_sid,
                  rrh.creation_date_time,
                  rrh.requisition_status_id,
                  rrh.recruiter_owner_user_sid,
                  rrh.valid_to_date,
                  rrh.closed_date_time
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history AS rrh
                WHERE rrh.valid_to_date = DATETIME("9999-12-31 23:59:59")
                QUALIFY row_number() OVER (PARTITION BY rrh.recruitment_requisition_sid, rrh.creation_date_time ORDER BY rrh.creation_date_time DESC) = 1
            ) AS bb
      ) AS b ON a.recruitment_requisition_sid = b.recruitment_requisition_sid
       AND a.row_rank1 + 1 = b.row_rank2

      LEFT OUTER JOIN (
        SELECT
          recruitment_requisition_sid,
          min(creation_date_time) AS creation_date_time
        FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE requisition_status_id = 1002
        GROUP BY 1
      ) ghrhist
      ON a.recruitment_requisition_sid = ghrhist.recruitment_requisition_sid
  );

--New Temp Table for Approval_date update

CREATE TEMPORARY TABLE approvaldate AS (
  --pull all historical open requisitions
  SELECT
    r.requisition_sid,
    CASE
      WHEN UPPER(recreq.source_system_code) = 'T' THEN COALESCE(rw.end_date, r.requisition_open_date)
      ELSE COALESCE(rrh.requisition_open_date, r.requisition_open_date)
    END AS requisition_open_date_tb
  FROM {{ params.param_hr_base_views_dataset_name }}.requisition r

  LEFT OUTER JOIN (
    SELECT
      requisition_sid,
      MAX(end_date) as end_date
    FROM {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
    WHERE DATE(valid_to_date) = '9999-12-31'
    GROUP BY 1
  ) rw --requisition approval date
  ON r.requisition_sid = rw.requisition_sid

  INNER JOIN (
    SELECT 
      *
    FROM(
      SELECT
        rr.recruitment_requisition_sid,
        rr.lawson_requisition_sid,
        o.accept_date,
        o.offer_sid,
        rr.process_level_code,
        ros.offer_status_desc,
        rr.approved_sw,
        s.submission_sid,
        rr.source_system_code,
        o.sequence_num,
        rr.lawson_requisition_num,
        rr.lawson_company_num
      FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission s
      ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
      and DATE(s.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer o
      ON s.submission_sid = o.submission_sid
      AND DATE(o.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status os
      ON os.offer_sid = o.offer_sid
      AND DATE(os.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
      ON ros.offer_status_id = os.offer_status_id

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow rw
      ON rr.workflow_id = rw.workflow_id

      WHERE DATE(rr.valid_to_date) = '9999-12-31'
      AND (UPPER(rw.workflow_code) NOT LIKE '%ACQ%' OR rw.workflow_code IS NULL)

      QUALIFY ROW_NUMBER() OVER(PARTITION BY rr.lawson_requisition_sid, rr.source_system_code ORDER BY o.last_modified_date DESC, o.last_modified_time DESC, o.capture_date DESC, rr.approved_sw DESC, rr.recruitment_requisition_sid DESC, o.sequence_num DESC, accept_date DESC)=1
    ) rr
    QUALIFY ROW_NUMBER() OVER(PARTITION BY rr.lawson_requisition_sid ORDER BY rr.source_system_code DESC) = 1
  )recreq --pulling the latest offer associated with the requisition. If we have the same Lawson_Requisition_SID from both sources (Taleo and ATS) is going to the data from Taleo.
  ON recreq.lawson_requisition_sid = r.requisition_sid
  
  LEFT OUTER JOIN (
    SELECT
      recruitment_requisition_sid,
      MIN(creation_date_time) AS requisition_open_date
    FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
    WHERE requisition_status_id = 1002
    GROUP BY 1
  ) rrh
  ON recreq.recruitment_requisition_sid = rrh.recruitment_requisition_sid

  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status rs
  ON recreq.lawson_requisition_sid = rs.requisition_sid
  AND DATE(rs.valid_to_date) = '9999-12-31'

  WHERE DATE(r.requisition_closed_date) <> '9999-12-31' --all closed requisitions
  AND UPPER(rs.status_code) <> 'WFINPROG' --closed can't be in progress
  and DATE(r.valid_to_date) = '9999-12-31'

  UNION ALL
--pulling all current open reqs
  SELECT
    r.requisition_sid,
    CASE
      WHEN UPPER(recreq.source_system_code) = 'T' THEN COALESCE(rw.end_date, r.requisition_open_date)
      ELSE COALESCE(rrh.requisition_open_date, r.requisition_open_date)
    END AS requisition_open_date_tb
  FROM {{ params.param_hr_base_views_dataset_name }}.requisition r

  LEFT OUTER JOIN (
    SELECT
      requisition_sid,
      MAX(end_date) as end_date
    FROM {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
    WHERE DATE(valid_to_date) = '9999-12-31'
    GROUP BY 1
  ) rw --requisition approval date
  ON r.requisition_sid = rw.requisition_sid

  INNER JOIN (
    SELECT 
      *
    FROM(
      SELECT
        rr.recruitment_requisition_sid,
        rr.lawson_requisition_sid,
        o.accept_date,
        o.offer_sid,
        rr.process_level_code,
        ros.offer_status_desc,
        rr.approved_sw,
        s.submission_sid,
        rr.source_system_code,
        o.sequence_num,
        rr.lawson_requisition_num,
        rr.lawson_company_num
      FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission s
      ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
      and DATE(s.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer o
      ON s.submission_sid = o.submission_sid
      AND DATE(o.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status os
      ON os.offer_sid = o.offer_sid
      AND DATE(os.valid_to_date) = '9999-12-31'

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
      ON ros.offer_status_id = os.offer_status_id

      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow rw
      ON rr.workflow_id = rw.workflow_id

      WHERE DATE(rr.valid_to_date) = '9999-12-31'
      AND (UPPER(rw.workflow_code) NOT LIKE '%ACQ%' OR rw.workflow_code IS NULL)

      QUALIFY ROW_NUMBER() OVER(PARTITION BY rr.lawson_requisition_sid, rr.source_system_code ORDER BY o.last_modified_date DESC, o.last_modified_time DESC, o.capture_date DESC, rr.approved_sw DESC, rr.recruitment_requisition_sid DESC, o.sequence_num DESC, accept_date DESC)=1
    ) rr
    QUALIFY ROW_NUMBER() OVER(PARTITION BY rr.lawson_requisition_sid ORDER BY rr.source_system_code DESC) = 1
  )recreq --pulling the latest offer associated with the requisition. If we have the same Lawson_Requisition_SID from both sources (Taleo and ATS) is going to the data from Taleo.
    ON recreq.lawson_requisition_sid = r.requisition_sid

  LEFT OUTER JOIN (
    SELECT
      recruitment_requisition_sid,
      MIN(creation_date_time) AS requisition_open_date
    FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
    WHERE requisition_status_id = 1002
    GROUP BY 1
  ) rrh
  ON recreq.recruitment_requisition_sid = rrh.recruitment_requisition_sid

  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status rs
  ON recreq.lawson_requisition_sid = rs.requisition_sid
  AND DATE(rs.valid_to_date) = '9999-12-31'

  WHERE DATE(r.requisition_closed_date) = '9999-12-31' --all open requisitions
  AND UPPER(rs.status_code) = 'WFAPPROVE' --must be approved open
  and DATE(r.valid_to_date) = '9999-12-31'
);

BEGIN TRANSACTION; 

  INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv (
    employee_sid,
    requisition_sid,
    position_sid,
    date_id,
    analytics_msr_sid,
    dept_sid,
    job_class_sid,
    job_code_sid,
    location_code,
    coid,
    company_code,
    functional_dept_num,
    sub_functional_dept_num,
    auxiliary_status_sid,
    employee_status_sid,
    key_talent_id,
    integrated_lob_id,
    action_code,
    action_reason_text,
    lawson_company_num,
    process_level_code,
    work_schedule_code,
    recruiter_owner_user_sid,
    requisition_approval_date,
    employee_num,
    metric_numerator_qty,
    metric_denominator_qty,
    source_system_code,
    dw_last_update_date_time)
    SELECT
        0 AS employee_sid,
        reqday.requisition_sid,
        reqday.position_sid,
        reqday.date_id,
        dm.analytics_msr_sid,
        reqday.dept_sid,
        jc.job_class_sid,
        jc.job_code_sid,
        reqday.location_code,
        gldc.coid,
        gldc.company_code,
        sf.functional_dept_num,
        sf.sub_functional_dept_num,
        0 AS auxiliary_status_sid,
        0 AS employee_status_sid,
        coalesce(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) AS key_talent_id,
        coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
        '0' AS action_code,
        '0' AS action_reason_text,
        reqday.lawson_company_num,
        reqday.process_level_code,
        reqday.work_schedule_code,
        reqday.recruiter_owner_user_sid,
        CAST(ad.requisition_open_date_tb AS DATE) AS requisition_approval_date,
        0 AS employee_num,
        1 AS metric_numerator_qty,
        0 AS metric_denominator_qty,
        reqday.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        ( --Closed Requisitions
          SELECT
              r.requisition_sid,
              r.process_level_code AS lawson_process_level,
              recreq.process_level_code AS taleo_process_level,
              recreq.recruitment_requisition_sid,
              rp.position_sid,
              r.process_level_code,
              r.lawson_company_num,
              r.location_code,
              r.work_schedule_code,
              rd.dept_sid,
              rs.status_sid,
              rs.status_code,
              d.date_id,
              recreq.accept_date,
              recreq.offer_sid,
              recreq.offer_status_desc,
              recreq.approved_sw,
              ts.recruiter_owner_user_sid,
              rw.end_date,
              r.requisition_open_date,
              ts.requisition_status_id,
              recreq.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.requisition AS r
              LEFT OUTER JOIN (
                SELECT
                    requisition_workflow.requisition_sid,
                    max(requisition_workflow.end_date) AS end_date
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
                  WHERE requisition_workflow.valid_to_date = DATETIME("9999-12-31 23:59:59")
                   AND (select max(requisition_workflow.end_date) from {{ params.param_hr_base_views_dataset_name }}.requisition_workflow) <> '9999-12-31'
                  GROUP BY 1
              ) AS rw ON r.requisition_sid = rw.requisition_sid
              INNER JOIN (
                SELECT
                    rr.recruitment_requisition_sid,
                    rr.lawson_requisition_sid,
                    o.accept_date,
                    o.offer_sid,
                    rr.process_level_code,
                    ros.offer_status_desc,
                    rr.approved_sw,
                    s.submission_sid,
                    rr.source_system_code,
                    o.sequence_num
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
                     AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
                     AND o.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
                     AND os.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw_0 ON rr.workflow_id = rw_0.workflow_id
                  WHERE rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
                   AND (rw_0.workflow_code IS NULL
                   OR upper(trim(rw_0.workflow_code)) NOT LIKE '%ACQ%')
                  QUALIFY row_number() OVER (PARTITION BY rr.lawson_requisition_sid, rr.source_system_code ORDER BY o.last_modified_date DESC, o.last_modified_time DESC, o.capture_date DESC, rr.approved_sw DESC, rr.recruitment_requisition_sid DESC, o.sequence_num DESC, o.accept_date DESC) = 1
              ) AS recreq ON recreq.lawson_requisition_sid = r.requisition_sid
              LEFT OUTER JOIN (
                SELECT
                    o.offer_sid,
                    o.submission_sid,
                    o.accept_date,
                    os.offer_status_id
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.offer AS o
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
                     AND os.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
                  WHERE upper(trim(ros.offer_status_desc)) IN(
                    'ACCEPTED'
                  )
                   AND o.valid_to_date = DATETIME("9999-12-31 23:59:59")
              ) AS offerstatus ON offerstatus.offer_sid = recreq.offer_sid

              LEFT JOIN approvaldate ad
                ON r.requisition_sid = ad.requisition_sid

              --Change the closed date to use GHR close date for cancelled requisitions HDM-2282 N Lamborn
              LEFT JOIN (
                Select
                  recruitment_requisition_sid,
                  max(cast(closed_date_time as date)) as requisition_closed_date
                FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
                WHERE requisition_status_id = 1005
                AND DATE(valid_to_date) = '9999-12-31'
                GROUP BY 1
              ) closedate
              ON recreq.recruitment_requisition_sid = closedate.recruitment_requisition_sid
              
              INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS d ON d.date_id >= ad.requisition_open_date_tb
               AND d.date_id <= COALESCE(closedate.requisition_closed_date, COALESCE(offerstatus.accept_date, r.requisition_closed_date)) --taking the date the offer was accepted, or if it wasn't accepted, when the requisition closed
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON recreq.lawson_requisition_sid = rs.requisition_sid
               AND rs.valid_to_date = DATETIME("9999-12-31 23:59:59")
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON rp.requisition_sid = r.requisition_sid
               AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON rd.requisition_sid = r.requisition_sid
               AND rd.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN ts ON recreq.recruitment_requisition_sid = ts.recruitment_requisition_sid
               AND d.date_id BETWEEN DATE(ts.creation_date_time) AND ts.status_end_date
            WHERE r.requisition_closed_date <> '9999-12-31'
             AND upper(trim(rs.status_code)) <> 'WFINPROG'
             AND r.valid_to_date = DATETIME("9999-12-31 23:59:59")
             AND d.date_id >= '2016-01-01'
          UNION DISTINCT
          SELECT --Open Requisitions
              r.requisition_sid,
              r.process_level_code AS lawson_process_level,
              recreq.process_level_code AS taleo_process_level,
              recreq.recruitment_requisition_sid,
              rp.position_sid,
              r.process_level_code,
              r.lawson_company_num,
              r.location_code,
              r.work_schedule_code,
              rd.dept_sid,
              rs.status_sid,
              rs.status_code,
              d.date_id,
              recreq.accept_date,
              recreq.offer_sid,
              recreq.offer_status_desc,
              recreq.approved_sw,
              ts.recruiter_owner_user_sid,
              rw.end_date,
              r.requisition_open_date,
              ts.requisition_status_id,
              recreq.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.requisition AS r
              LEFT OUTER JOIN (
                SELECT
                    requisition_workflow.requisition_sid,
                    max(requisition_workflow.end_date) AS end_date
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
                  WHERE requisition_workflow.valid_to_date = DATETIME("9999-12-31 23:59:59")
                   AND requisition_workflow.end_date <> '9999-12-31'
                  GROUP BY 1
              ) AS rw ON r.requisition_sid = rw.requisition_sid
              INNER JOIN (
                SELECT
                    rr.recruitment_requisition_sid,
                    rr.lawson_requisition_sid,
                    o.accept_date,
                    o.offer_sid,
                    rr.process_level_code,
                    ros.offer_status_desc,
                    rr.approved_sw,
                    s.submission_sid,
                    rr.source_system_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
                     AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
                     AND o.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
                     AND os.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw_0 ON rr.workflow_id = rw_0.workflow_id
                  WHERE rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
                   AND (rw_0.workflow_code IS NULL
                   OR upper(trim(rw_0.workflow_code)) NOT LIKE '%ACQ%')
                  QUALIFY row_number() OVER (PARTITION BY rr.lawson_requisition_sid, rr.source_system_code ORDER BY o.last_modified_date DESC, o.last_modified_time DESC, o.capture_date DESC, rr.approved_sw DESC, rr.recruitment_requisition_sid DESC, o.sequence_num DESC, o.accept_date DESC) = 1
              ) AS recreq ON recreq.lawson_requisition_sid = r.requisition_sid
              LEFT OUTER JOIN (
                SELECT
                    o.offer_sid,
                    o.submission_sid,
                    o.accept_date,
                    os.offer_status_id
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.offer AS o
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
                     AND os.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
                  WHERE upper(trim(ros.offer_status_desc)) IN(
                    'ACCEPTED'
                  )
                   AND o.valid_to_date = DATETIME("9999-12-31 23:59:59")
              ) AS offerstatus ON offerstatus.offer_sid = recreq.offer_sid
              
              LEFT JOIN approvaldate ad
                ON r.requisition_sid = ad.requisition_sid
              
              INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS d ON d.date_id >= ad.requisition_open_date_tb
               AND d.date_id <= coalesce(offerstatus.accept_date, current_date('US/Central'))
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON recreq.lawson_requisition_sid = rs.requisition_sid
               AND rs.valid_to_date = DATETIME("9999-12-31 23:59:59")
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON rp.requisition_sid = r.requisition_sid
               AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON rd.requisition_sid = r.requisition_sid
               AND rd.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN ts ON recreq.recruitment_requisition_sid = ts.recruitment_requisition_sid
               AND d.date_id BETWEEN DATE(ts.creation_date_time) AND ts.status_end_date
            WHERE r.requisition_closed_date = '9999-12-31'
             AND upper(trim(rs.status_code)) = 'WFAPPROVE'
             AND r.valid_to_date = DATETIME("9999-12-31 23:59:59")
             AND d.date_id >= '2016-01-01'
        ) AS reqday
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON reqday.position_sid = jp.position_sid
         AND reqday.date_id BETWEEN jp.eff_from_date AND jp.eff_to_date
         AND jp.valid_to_date = DATETIME("9999-12-31 23:59:59")
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON jp.gl_company_num = gldc.gl_company_num
         AND jp.account_unit_num = gldc.account_unit_num
         AND gldc.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN -- AND jp.Process_Level_Code = gldc.Process_Level_Code
        {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON gldc.coid = ff.coid
         AND gldc.company_code = ff.company_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
         AND jc.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS sf ON sf.dept_num = gldc.dept_num
         AND sf.coid = gldc.coid
         AND sf.company_code = gldc.company_code
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS df ON sf.functional_dept_num = df.functional_dept_num
        INNER JOIN {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure AS dm ON upper(dm.analytics_msr_name_child) = 'HR_LAWSON_OPENINGS'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON reqday.dept_sid = dept.dept_sid
         AND dept.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1 ON reqday.process_level_code = mat1.process_level_code
         AND dept.dept_code = mat1.dept_code
         AND mat1.match_level_num = 1
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat2 ON ff.lob_code = mat2.lob_code
         AND ff.sub_lob_code = mat2.sub_lob_code
         AND mat2.match_level_num = 2
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat3 ON upper(trim(df.functional_dept_desc)) = upper(trim(mat3.functional_dept_desc))
         AND upper(trim(sf.sub_functional_dept_desc)) = upper(trim(mat3.sub_functional_dept_desc))
         AND mat3.match_level_num = 3
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4 ON reqday.process_level_code = mat4.process_level_code
         AND mat4.match_level_num = 4
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1 ON rkeyt1.match_level_num = 1
         AND jc.job_code = rkeyt1.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt1.job_code_desc))
         AND upper(trim(jp.position_code_desc)) LIKE 'ACMO%'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2 ON rkeyt2.match_level_num = 2
         AND jc.job_code = rkeyt2.job_code
         AND ff.lob_code = rkeyt2.lob_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt2.job_code_desc))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3 ON rkeyt3.match_level_num = 3
         AND jc.job_code = rkeyt3.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt3.job_code_desc))
         AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt3.job_title_text))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4 ON rkeyt4.match_level_num = 4
         AND jc.job_code = rkeyt4.job_code
         AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt4.job_title_text))
         AND reqday.process_level_code = rkeyt4.process_level_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5 ON rkeyt5.match_level_num = 5
         AND jc.job_code = rkeyt5.job_code
         AND reqday.process_level_code = rkeyt5.process_level_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6 ON rkeyt6.match_level_num = 6
         AND jc.job_code = rkeyt6.job_code
         AND reqday.process_level_code = rkeyt6.process_level_code
         AND upper(trim(jp.position_code_desc)) LIKE 'DIR PRGM%'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7 ON rkeyt6.match_level_num = 7
         AND jc.job_code = rkeyt7.job_code
         AND reqday.process_level_code = rkeyt7.process_level_code
         AND upper(trim(dept.dept_code)) BETWEEN '70000' AND '79999'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8 ON rkeyt8.match_level_num = 8
         AND jc.job_code = rkeyt8.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt8.job_code_desc))
        LEFT OUTER JOIN approvaldate ad
         ON ad.requisition_sid = reqday.requisition_sid
      WHERE reqday.requisition_status_id NOT IN(
        10, 12, 1005
      )
       AND reqday.lawson_company_num <> 300
      QUALIFY row_number() OVER (PARTITION BY reqday.requisition_sid, reqday.position_sid, reqday.date_id ORDER BY reqday.source_system_code DESC) = 1
  ;


/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Employee_SID, Requisition_SID, Position_SID, Date_Id, Analytics_Msr_Sid
  from {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv
  group by Employee_SID, Requisition_SID, Position_SID, Date_Id, Analytics_Msr_Sid
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table :{{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv');
ELSE  
  COMMIT  TRANSACTION;
END IF;


END;
