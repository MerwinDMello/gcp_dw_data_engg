create or replace view `{{ params.param_hr_base_views_dataset_name }}.submission_tracking`
AS SELECT
    submission_tracking.submission_tracking_sid,
    submission_tracking.valid_from_date,
    submission_tracking.candidate_profile_sid,
    submission_tracking.submission_tracking_num,
    submission_tracking.creation_date_time,
    submission_tracking.event_date_time,
    submission_tracking.event_detail_text,
    submission_tracking.submission_event_id,
    submission_tracking.tracking_user_sid,
    submission_tracking.tracking_step_id,
    submission_tracking.tracking_workflow_id,
    submission_tracking.step_reverted_ind,
    submission_tracking.sub_status_desc,
	submission_tracking.moved_by_text,
    submission_tracking.valid_to_date,
    submission_tracking.source_system_code,
    submission_tracking.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.submission_tracking;