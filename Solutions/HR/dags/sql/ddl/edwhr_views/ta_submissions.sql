
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ta_submissions AS SELECT
      s.candidate_profile_sid,
      s.candidate_profile_num,
      s.candidate_num,
      s.submission_sid,
      s.submission_uid,
      s.recruitment_requisition_num_text,
      s.recruitment_requisition_sid,
      s.lawson_requisition_num,
      s.matched_from_requisition_num,
      s.requisition_sid,
      s.dept_sid,
      s.coid,
      s.creation_date,
      s.completion_date,
      CASE
        WHEN s.completion_date IS NULL THEN 0
        ELSE 1
      END AS is_completed_sw,
      s.recruitment_source_desc,
      s.recruitment_source_type_desc,
      s.recruitment_source_auto_filled_sw,
      s.profile_medium_desc,
      s.status_code AS emp_status,
      s.open_fte_percent AS fte_value,
      s.job_schedule_desc,
      s.paid_source_flag AS paid_source,
      s.submission_status_name,
      s.current_submission_step_name AS step_name,
      s.quality_flag AS quality_ind,
      s.motive_name,
      1 AS sub_measure_qty,
      s.new_grad_flag AS offer_grad_flag,
      s.gender_name,
      s.ethnicity_name,
      s.veteran_desc,
      s.veteran_spouse_flag,
      s.disability_flag AS disability_ind,
      s.matched_candidate_flag AS matched_candidate,
      s.hrr_duration_cnt AS hrr_duration,
      s.hmr_duration_cnt AS hmr_duration,
      s.interview_duration_cnt AS interview_duration,
      s.offer_duration_cnt AS offer_duration,
      s.candidate_intl_ext_flag AS candidate_ie_ind,
      s.source_system_code
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_submission AS s
  ;

