CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.fwd_candidatesbyreq AS SELECT
    s.taleo_requisition_num AS recruitment_requisition_num,
    rs.status_desc AS recruitment_status,
    s.taleo_candidate_id AS candidate_id,
    s.current_submission_status,
    s.current_submission_step,
    o.extend_date,
    o.accept_date,
    o.start_date,
    '1' AS active_count
  FROM
    `hca-hin-dev-cur-hr`.edwhr_bi_views.factsubmissions AS s
    LEFT OUTER JOIN (
      SELECT
          ao.offer_sid,
          ao.submission_sid,
          ao.extend_date,
          ao.accept_date,
          ao.start_date,
          ao.last_modified_date,
          ao.capture_date,
          ao.sequence_num,
          ao.valid_to_date,
          rank() OVER (PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) AS active
        FROM
          {{ params.param_hr_base_views_dataset_name }}.offer AS ao
        WHERE date(ao.valid_to_date) = '9999-12-31'
         AND ao.accept_date IS NOT NULL
    ) AS o ON s.submission_sid = o.submission_sid
     AND o.active = 1
    LEFT OUTER JOIN (
      SELECT
          rr.recruitment_requisition_sid,
          rr.recruitment_requisition_num_text,
          recstatus.status_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS rrs ON rr.recruitment_requisition_sid = rrs.recruitment_requisition_sid
           AND date(rrs.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_requisition_status AS recstatus ON rrs.requisition_status_id = recstatus.requisition_status_id
        WHERE date(rr.valid_to_date) = '9999-12-31'
    ) AS rs ON s.taleo_requisition_num = rs.recruitment_requisition_num_text
  WHERE date(s.submission_completed_date) > '2020-01-01'
   AND s.current_submission_status NOT IN(
    'Rejected', 'Candidate Withdrew', 'Rescinded'
  )
   AND upper(s.current_submission_status) NOT LIKE '%DISPOSIT%'
;
