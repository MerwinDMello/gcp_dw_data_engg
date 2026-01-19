

--  view to allow for analysts to match up Lawson req number with ATS req number for joining to BOBJ reports
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.ats_requisitions AS SELECT
    r.lawson_company_num AS company,
    r.process_level_code AS process_level_code,
    r.requisition_num AS requisition_num,
    rr.lawson_requisition_num AS lawson_requisition_num,
    r.requisition_origination_date,
    r.requisition_open_date,
    rr.requisition_num AS ghr_req_num
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
          recr.source_system_code,
          o.accept_date,
          o.offer_sid,
          recr.process_level_code,
          ros.offer_status_desc,
          recr.approved_sw,
          s.submission_sid
        FROM
          {{ params.param_hr_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
        WHERE date(recr.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
  WHERE date(r.valid_to_date) = '9999-12-31'
   AND upper(rr.source_system_code) = 'B'
;