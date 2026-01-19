-- Restore query for edwhr_staging.submission_tracking_wrk

insert into {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk(file_date, submission_tracking_sid, candidate_profile_sid, submission_tracking_num, creation_date_time,
  event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, sub_status_desc, step_reverted_ind,
  source_system_code, dw_last_update_date_time ) select file_date, submission_tracking_sid, candidate_profile_sid, submission_tracking_num, creation_date_time,
  event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, sub_status_desc, step_reverted_ind,
  source_system_code, dw_last_update_date_time from prod_support.submission_tracking_wrk_bkp_prod;

-- Restore query for edwhr.submission_tracking

insert into {{ params.param_hr_core_dataset_name }}.submission_tracking (submission_tracking_sid, valid_from_date, candidate_profile_sid, submission_tracking_num, 
creation_date_time, event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, step_reverted_ind,  
sub_status_desc,valid_to_date, source_system_code, dw_last_update_date_time) select submission_tracking_sid, valid_from_date, candidate_profile_sid, submission_tracking_num, creation_date_time, event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, 
step_reverted_ind,sub_status_desc, valid_to_date, source_system_code, dw_last_update_date_time from prod_support.submission_tracking_bkp_prod;



