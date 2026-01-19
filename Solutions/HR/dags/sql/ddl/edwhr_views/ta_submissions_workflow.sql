
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ta_submissions_workflow AS SELECT
      st.stepstatus_uid,
      st.candidate_profile_sid,
      st.taleo_submission_id AS candidate_profile_num,
      st.taleo_candidate_id AS candidate_num,
      st.submission_sid,
      st.taleo_requisition_num AS recruitment_requisition_num_text,
      st.recruitment_requisition_sid,
      st.lawson_requisition_num,
      st.requisition_sid,
      st.tracking_step_id,
      st.creation_date,
      st.completion_date,
      st.step_short_name,
      st.step_name,
      st.submission_status_name,
      st.wf_status_start,
      st.wf_status_end,
      st.duration AS duration_dmhs,
      st.wf_row_rank,
      st.step_reverted_ind
    FROM
      {{ params.param_hr_bi_views_dataset_name }}.factstepstatus AS st
  ;

