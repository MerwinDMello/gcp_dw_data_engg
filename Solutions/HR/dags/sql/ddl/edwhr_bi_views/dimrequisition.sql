-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/dimrequisition.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimrequisition AS SELECT
    concat(coalesce(r.requisition_sid, 00000), coalesce(cp.candidate_profile_sid, 00000)) AS req_app_uid,
    r.requisition_sid,
    rr.recruitment_requisition_sid,
    CASE
      WHEN upper(rr.source_system_code) = 'T' THEN rr.lawson_requisition_num
      WHEN upper(rr.source_system_code) = 'B' THEN rr.requisition_num
    END AS requisition_num,
    rr.recruitment_requisition_num_text AS taleo_requisition_num,
    cp.candidate_profile_sid,
    r.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
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
          s.submission_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
        WHERE date(recr.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
     AND r.process_level_code = rr.process_level_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
     AND date(s.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON s.candidate_profile_sid = cp.candidate_profile_sid
     AND cp.completion_date IS NOT NULL
     AND date(cp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
     AND date(rj.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND date(d.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
     AND date(rp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
     AND date(jp.valid_to_date) = '9999-12-31'
     AND date(jp.eff_to_date) = '9999-12-31'
     AND upper(jp.active_dw_ind) = 'Y'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND d.dept_sid = hrdr.dept_sid
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
  WHERE date(r.valid_to_date) = '9999-12-31'
   AND r.requisition_open_date BETWEEN '2018-10-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
   AND cp.candidate_profile_sid IS NOT NULL
;
