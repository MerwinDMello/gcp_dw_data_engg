-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/{{ params.param_hr_views_dataset_name }}/fact_submission.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.fact_submission AS SELECT
    a.submission_sid,
    a.candidate_profile_num,
    a.candidate_num,
    a.candidate_profile_sid,
    a.candidate_sid,
    a.submission_uid,
    a.dept_sid,
    a.company_code,
    a.coid,
    a.coid_dept_uid,
    a.creation_date,
    a.creation_date_time,
    a.completion_date,
    a.completion_date_time,
    a.recruitment_source_desc,
    a.recruitment_source_type_desc,
    a.recruitment_source_auto_filled_sw,
    a.profile_medium_desc,
    a.recruitment_requisition_num_text,
    a.recruitment_requisition_sid,
    a.lawson_requisition_num,
    a.ghr_requisition_num,
    a.matched_from_requisition_num,
    a.requisition_sid,
    a.status_code,
    a.first_offer_start_date,
    a.open_fte_percent,
    a.job_schedule_desc,
    a.paid_source_flag,
    a.last_modified_date,
    a.submission_status_name,
    a.current_submission_step_name,
    a.quality_flag,
    a.motive_name,
    a.new_grad_flag,
    a.gender_name,
    a.ethnicity_name,
    a.veteran_desc,
    a.veteran_spouse_flag,
    a.disability_flag,
    a.hrr_duration_cnt,
    a.hmr_duration_cnt,
    a.interview_duration_cnt,
    a.offer_duration_cnt,
    a.matched_candidate_flag,
    a.candidate_intl_ext_flag,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_submission AS a
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
