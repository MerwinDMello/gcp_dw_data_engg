
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.submission_tracking AS SELECT
      a.submission_tracking_sid,
      a.valid_from_date,
      a.candidate_profile_sid,
      a.submission_tracking_num,
      a.creation_date_time,
      a.event_date_time,
      a.event_detail_text,
      a.submission_event_id,
      a.tracking_user_sid,
      a.tracking_step_id,
      a.tracking_workflow_id,
      a.step_reverted_ind,
      a.sub_status_desc,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS a
  ;

