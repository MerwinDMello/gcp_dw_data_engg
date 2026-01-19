create table if not exists `{{ params.param_hr_core_dataset_name }}.fact_step_status_2248`
(
  step_status_sid NUMERIC NOT NULL OPTIONS(description="A unique identifier for each step. This field is a combination of the Submission_SID and Candidate_Profile_SID"),
  step_seq_num INT64 NOT NULL OPTIONS(description="Assigned sequence number for multiple records for the same candidate profile and submission."),
  candidate_profile_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each candidate profile."),
  candidate_profile_num NUMERIC OPTIONS(description="A unique identifier for each candidate profile from the source."),
  candidate_num INT64 OPTIONS(description="The unique number for a candidate coming from the source."),
  submission_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each submission "),
  recruitment_requisition_num_text STRING OPTIONS(description="A unique value coming from the source that combines the location code and lawson requisition number."),
  recruitment_requisition_sid INT64 OPTIONS(description="A unique identifier for each requisition coming from the recruitment system."),
  lawson_requisition_sid INT64 OPTIONS(description="A unique identifier for the Lawson requisition."),
  ghr_requisition_num INT64 OPTIONS(description="The requisition_num for GHR records"),
  requisition_sid INT64 OPTIONS(description="Unique identifier of an lawson generated requisition."),
  tracking_step_id INT64 OPTIONS(description="A unique identifier for each step an submission can take in the application process."),
  creation_date DATE OPTIONS(description="Date the submission profile was created by the candidate."),
  creation_date_time DATETIME OPTIONS(description="Date time the submission profile was created by the candidate."),
  completion_date DATE OPTIONS(description="Date the submission profile was completed by the candidate."),
  completion_date_time DATETIME OPTIONS(description="Date time the submission profile was completed by the candidate."),
  step_name STRING OPTIONS(description="The name for each step a submission can take."),
  step_short_name STRING OPTIONS(description="Contains the step short name."),
  submission_status_name STRING OPTIONS(description="A name for each status of the submission process."),
  sla_compliance_status_text STRING OPTIONS(description="Contains the SLA Compliance which is assgined based on the step and the duration time it took to complete this step. "),
  sub_status_desc STRING OPTIONS(description="Contains the sub status description. "),
  moved_by_text STRING OPTIONS(description="Contains the identifier of the person or system that made the change"),
  step_status_start_date_time DATETIME OPTIONS(description="Contains the event start date."),
  step_status_end_date_time DATETIME OPTIONS(description="Contains the event end date."),
  company_code STRING OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes"),
  coid STRING OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities."),
  dept_num STRING OPTIONS(description="It is the HCA department number of an company"),
  duration_days_cnt NUMERIC OPTIONS(description="Number of days it took to complete the step."),
  step_reverted_ind STRING OPTIONS(description="Designates whether a step in the application process has been reverted. Y = reverted, N = not reverted."),
  non_working_day_cnt INT64 OPTIONS(description="Contains the count of non working days. (weekend or holiday)."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY step_status_sid, step_seq_num
OPTIONS(
  description="This table contains facts and information needed for HRG to report all the step status details. For example - Step names and Duration"
);

INSERT INTO `{{ params.param_hr_core_dataset_name }}.fact_step_status_2248`
SELECT
  step_status_sid,
  step_seq_num,
  candidate_profile_sid,
  candidate_profile_num,
  candidate_num,
  submission_sid,
  recruitment_requisition_num_text,
  recruitment_requisition_sid,
  lawson_requisition_sid,
  null as ghr_requisition_num,
  requisition_sid,
  tracking_step_id,
  creation_date,
  null as creation_date_time,
  completion_date,
  null as completion_date_time,
  step_name,
  step_short_name,
  submission_status_name,
  sla_compliance_status_text,
  sub_status_desc,
  null as moved_by_text,
  step_status_start_date_time,
  step_status_end_date_time,
  company_code,
  coid,
  dept_num,
  duration_days_cnt,
  step_reverted_ind,
  non_working_day_cnt,
  source_system_code,
  dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.fact_step_status;

ALTER TABLE `{{ params.param_hr_core_dataset_name }}.fact_step_status`
RENAME TO `fact_step_status_pre_2248`;

ALTER TABLE `{{ params.param_hr_core_dataset_name }}.fact_step_status_2248`
RENAME TO `fact_step_status`;

CREATE OR REPLACE VIEW
  {{ params.param_hr_base_views_dataset_name }}.fact_step_status AS
SELECT
  step_status_sid,
  step_seq_num,
  candidate_profile_sid,
  candidate_profile_num,
  candidate_num,
  submission_sid,
  recruitment_requisition_num_text,
  recruitment_requisition_sid,
  lawson_requisition_sid,
  ghr_requisition_num,
  requisition_sid,
  tracking_step_id,
  creation_date,
  creation_date_time,
  completion_date,
  completion_date_time,
  step_name,
  step_short_name,
  submission_status_name,
  sla_compliance_status_text,
  sub_status_desc,
  moved_by_text,
  step_status_start_date_time,
  step_status_end_date_time,
  company_code,
  coid,
  dept_num,
  duration_days_cnt,
  step_reverted_ind,
  non_working_day_cnt,
  source_system_code,
  dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.fact_step_status;

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

create or replace view {{ params.param_hr_bi_views_dataset_name }}.factstepstatus as SELECT
    concat(trim(CAST(fact_step_status.submission_sid as STRING)), trim(CAST(fact_step_status.candidate_profile_sid as STRING))) AS stepstatus_uid,
    fact_step_status.candidate_profile_sid,
    fact_step_status.candidate_profile_num AS taleo_submission_id,
    fact_step_status.candidate_num AS taleo_candidate_id,
    fact_step_status.submission_sid,
    fact_step_status.recruitment_requisition_num_text AS taleo_requisition_num,
    fact_step_status.recruitment_requisition_sid,
    fact_step_status.lawson_requisition_sid AS lawson_requisition_num,
    fact_step_status.ghr_requisition_num,
    fact_step_status.requisition_sid,
    fact_step_status.tracking_step_id,
    fact_step_status.creation_date,
    EXTRACT(TIME FROM fact_step_status.creation_date_time) AS creation_time,
    fact_step_status.completion_date,
    EXTRACT(TIME FROM fact_step_status.completion_date_time) AS completion_time,
    fact_step_status.step_short_name,
    fact_step_status.step_name,
    fact_step_status.submission_status_name,
    fact_step_status.sub_status_desc,
    fact_step_status.moved_by_text,
    fact_step_status.sla_compliance_status_text,
    fact_step_status.step_status_start_date_time AS wf_status_start,
    fact_step_status.step_status_end_date_time  AS wf_status_end,
    fact_step_status.coid,
    concat(coalesce(fact_step_status.coid, '00000'), coalesce(fact_step_status.dept_num, '000')) AS coid_uid,
    fact_step_status.duration_days_cnt AS duration,
    fact_step_status.non_working_day_cnt AS non_working_days,
    fact_step_status.step_seq_num AS wf_row_rank,
    fact_step_status.step_reverted_ind,
    fact_step_status.source_system_code

  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_step_status 
;