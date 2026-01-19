-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/{{ params.param_hr_views_dataset_name }}/fact_step_status.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.fact_step_status AS SELECT
    a.step_status_sid,
    a.step_seq_num,
    a.candidate_profile_sid,
    a.candidate_profile_num,
    a.candidate_num,
    a.submission_sid,
    a.recruitment_requisition_num_text,
    a.recruitment_requisition_sid,
    a.lawson_requisition_sid,
    a.ghr_requisition_num,
    a.requisition_sid,
    a.tracking_step_id,
    a.creation_date,
    a.creation_date_time,
    a.completion_date,
    a.completion_date_time,
    a.step_name,
    a.step_short_name,
    a.submission_status_name,
    a.sla_compliance_status_text,
    a.sub_status_desc,
    a.moved_by_text,
    a.step_status_start_date_time,
    a.step_status_end_date_time,
    a.company_code,
    a.coid,
    a.dept_num,
    a.duration_days_cnt,
    a.step_reverted_ind,
    a.non_working_day_cnt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_step_status AS a
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;