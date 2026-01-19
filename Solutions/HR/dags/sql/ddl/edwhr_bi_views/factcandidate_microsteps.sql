create or replace view {{ params.param_hr_bi_views_dataset_name }}.factcandidate_microsteps as SELECT
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    02/21/2021
    --  =============================================
    cpm.snapshot_date AS date_id,
    concat(coalesce(trim(cast(r.requisition_sid as string)), '00000'), coalesce(trim(cast(cp.candidate_profile_sid as string)), '00000')) AS req_app_uid,
    cpm.candidate_profile_sid,
    rr.recruitment_requisition_sid,
    upper(rr.recruitment_requisition_num_text) AS taleo_requisition_num,
    r.requisition_sid,
    r.requisition_num AS lawson_requisition_num,
    upper(r.process_level_code) AS process_level_code,
    c.candidate_num AS candidate_id,
    round2_start.min_step_start AS round2_date,
    p.pathway_num,
    upper(p.pathway_name) AS pathway_name,
    CASE
      WHEN p.pathway_num = 1 THEN '15'
      WHEN p.pathway_num = 3 THEN '19'
      WHEN p.pathway_num = 9 THEN '11'
      ELSE CAST(NULL as STRING)
    END AS path_sla,
    cpm.microstep_num,
    upper(ms.microstep_name) AS microstep_name,
    sla2.sla_day_cnt AS ms_sla,
    upper(sla2.day_type_code) AS day_type_code,
    cpm.microstep_start_date_time,
    cpm.microstep_end_date_time,
    CASE
      WHEN date_diff(DATE(cpm.microstep_end_date_time), DATE(cpm.microstep_start_date_time), DAY) > 365 THEN CAST(timestamp_diff(cpm.microstep_end_date_time, cpm.microstep_start_date_time, DAY) as NUMERIC)
      ELSE CAST(timestamp_diff(cpm.microstep_end_date_time, cpm.microstep_start_date_time, HOUR) as NUMERIC) / 24
    END AS step_duration,
    cpm.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep_snapshot AS cpm
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON cpm.candidate_profile_sid = cp.candidate_profile_sid
     AND DATE(cp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_pathway AS p ON cpm.pathway_id = p.pathway_id
    LEFT OUTER JOIN (
      SELECT DISTINCT
          microstep_pathway_sla.pathway_id,
          sum(microstep_pathway_sla.sla_day_cnt) AS path_sla
        FROM
          {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
        GROUP BY 1
    ) AS sla ON cpm.pathway_id = sla.pathway_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON cp.candidate_sid = c.candidate_sid
     AND DATE(c.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS person ON c.candidate_sid = person.candidate_sid
     AND DATE(person.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN (
      SELECT DISTINCT
          microstep_pathway_sla.microstep_num,
          microstep_pathway_sla.pathway_id,
          microstep_pathway_sla.sla_day_cnt,
          microstep_pathway_sla.day_type_code
        FROM
          {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
    ) AS sla2 ON cpm.microstep_num = sla2.microstep_num
     AND p.pathway_id = sla2.pathway_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_microstep AS ms ON cpm.microstep_num = ms.microstep_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON cp.candidate_profile_sid = s.candidate_profile_sid
     AND DATE(s.valid_to_date) = DATE('9999-12-31')
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
          s_0.submission_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s_0 ON recr.recruitment_requisition_sid = s_0.recruitment_requisition_sid
           AND DATE(s_0.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s_0.submission_sid = o.submission_sid
           AND DATE(o.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND DATE(os.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
        WHERE DATE(recr.valid_to_date) = DATE('9999-12-31')
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
    LEFT OUTER JOIN (
      SELECT
          requisition.requisition_sid,
          requisition.valid_from_date,
          requisition.valid_to_date,
          requisition.hr_company_sid,
          requisition.application_status_sid,
          requisition.lawson_company_num,
          requisition.process_level_code,
          requisition.location_code,
          requisition.requisition_num,
          requisition.requisition_desc,
          requisition.requisition_eff_date,
          requisition.requisition_open_date,
          requisition.requisition_closed_date,
          requisition.requisition_origination_date,
          requisition.originator_login_3_4_code,
          requisition.position_needed_date,
          requisition.job_opening_cnt,
          requisition.open_fte_percent,
          requisition.filled_fte_percent,
          requisition.last_update_date,
          requisition.replacement_employee_num,
          requisition.replacement_employee_sid,
          requisition.work_schedule_code,
          requisition.union_code,
          requisition.active_dw_ind,
          requisition.security_key_text,
          requisition.source_system_code,
          requisition.dw_last_update_date_time
        FROM
          {{ params.param_hr_base_views_dataset_name }}.requisition
        WHERE DATE(requisition.valid_to_date) = DATE('9999-12-31')
    ) AS r ON rr.lawson_requisition_sid = r.requisition_sid
     AND DATE(r.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN
    (
      SELECT DISTINCT
       -- --Add Round 2 Date
          fin.recruitment_requisition_sid,
          fin.candidate_profile_sid,
          min(fin.event_start) AS min_step_start,
          max(fin.event_end) AS max_step_end
        FROM
          (
            SELECT
                sub.recruitment_requisition_sid,
                sub.candidate_profile_sid,
                sub.submission_sid,
                st.step_short_name,
                coalesce(str.event_date_time, str.creation_date_time) AS event_start,
                lag(coalesce(str.event_date_time, str.creation_date_time), 1) OVER (PARTITION BY str.candidate_profile_sid ORDER BY coalesce(str.event_date_time, str.creation_date_time) DESC) AS event_end
              FROM
                {{ params.param_hr_base_views_dataset_name }}.submission AS sub
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS str ON sub.candidate_profile_sid = str.candidate_profile_sid
                 AND DATE(str.valid_to_date) = DATE('9999-12-31')
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS st ON str.tracking_step_id = st.step_id
              WHERE DATE(str.valid_to_date) = DATE('9999-12-31')
               AND DATE(sub.valid_to_date) = DATE('9999-12-31')
               AND sub.recruitment_requisition_sid IS NOT NULL
               AND str.tracking_step_id IS NOT NULL
          ) AS fin
        WHERE upper(fin.step_short_name) = 'ROUND 2'
        GROUP BY 1, 2
    ) AS round2_start ON rr.recruitment_requisition_sid = round2_start.recruitment_requisition_sid
     AND cpm.candidate_profile_sid = round2_start.candidate_profile_sid
    INNER JOIN 
    (
      SELECT
          -- -- Added snapshot date join to only pull the most recent snapshots 03/11/21
          max(candidate_pathway_microstep_snapshot.snapshot_date) AS maxsnap
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep_snapshot
    ) AS snapdt ON cpm.snapshot_date = snapdt.maxsnap
  GROUP BY 1, 6, cp.candidate_profile_sid, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, sla.path_sla, 14, 15, 16, 17, 18, 19, 21
