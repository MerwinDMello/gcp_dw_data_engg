CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_wrk_nt
(
  file_date DATE,
  submission_sid INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  submission_num NUMERIC,
  last_modified_date DATE,
  new_submission_sw INT64,
  candidate_sid INT64,
  recruitment_requisition_sid INT64,
  candidate_profile_sid INT64,
  current_submission_status_id INT64 NOT NULL,
  current_submission_step_id INT64 NOT NULL,
  current_submission_workflow_id INT64 NOT NULL,
  requisition_num INT64,
  job_application_num INT64,
  candidate_num INT64,
  matched_from_requisition_num INT64,
  matched_candidate_flag STRING,
  submission_source_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);

INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_wrk_nt(file_date,
submission_sid,
valid_from_date,
valid_to_date,
submission_num,
last_modified_date,
new_submission_sw,
candidate_sid,
recruitment_requisition_sid,
candidate_profile_sid,
current_submission_status_id,
current_submission_step_id,
current_submission_workflow_id,
requisition_num,
job_application_num,
candidate_num,
matched_from_requisition_num,
submission_source_code,
source_system_code,
dw_last_update_date_time)
 SELECT
file_date,
submission_sid,
valid_from_date,
valid_to_date,
submission_num,
last_modified_date,
new_submission_sw,
candidate_sid,
recruitment_requisition_sid,
candidate_profile_sid,
current_submission_status_id,
current_submission_step_id,
current_submission_workflow_id,
requisition_num,
job_application_num,
candidate_num,
matched_from_requisition_num,
submission_source_code,
source_system_code,
dw_last_update_date_time
FROM {{ params.param_hr_stage_dataset_name }}.submission_wrk;

ALTER TABLE {{ params.param_hr_stage_dataset_name }}.submission_wrk
	RENAME TO submission_wrk_old;
ALTER TABLE {{ params.param_hr_stage_dataset_name }}.submission_wrk_nt
	RENAME TO submission_wrk;


CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.submission_nt
(
submission_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each submission ")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME NOT NULL OPTIONS(description="Date on which the record was invalidated.")
, submission_num NUMERIC OPTIONS(description="A unique identifier for each submission from the source.")
, last_modified_date DATE OPTIONS(description="Date the submission was last modified.")
, new_submission_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate.")
, recruitment_requisition_sid INT64 OPTIONS(description="A unique identifier for each requisition coming from the recruitment system.")
, candidate_profile_sid INT64 OPTIONS(description="A unique identifier for each candidate profile.")
, current_submission_status_id INT64 NOT NULL OPTIONS(description="A unique identifier for status of the submission process.")
, current_submission_step_id INT64 NOT NULL OPTIONS(description="A unique identifier for each step a submission can take.")
, current_submission_workflow_id INT64 NOT NULL OPTIONS(description="A unique identifier for the workflow of current submission")
, requisition_num INT64 OPTIONS(description="Unique id of an lawson generated requisition.")
, job_application_num INT64 OPTIONS(description="Number corresponding with the job application filled out by the candidate. This number only changes when the original application is deleted or withdrawn.")
, candidate_num INT64 OPTIONS(description="The unique number for a candidate coming from the source.")
, matched_from_requisition_num INT64 OPTIONS(description="Contains the requisition number that a candidate originally applied to as recruiters can sometimes move applications to other requisitions.")
, matched_candidate_flag STRING OPTIONS(description="Indicates if this is the application number with which the candidate originally applied.")
, submission_source_code STRING OPTIONS(description="Code from the source that shows where an applicant found a requisition.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_sid
OPTIONS(description="Contains details for  the submission application of a candidate to a requisition");

INSERT INTO {{ params.param_hr_core_dataset_name }}.submission_nt(
submission_sid,
valid_from_date,
valid_to_date,
submission_num,
last_modified_date,
new_submission_sw,
candidate_sid,
recruitment_requisition_sid,
candidate_profile_sid,
current_submission_status_id,
current_submission_step_id,
current_submission_workflow_id,
requisition_num,
job_application_num,
candidate_num,
matched_from_requisition_num,
matched_candidate_flag,
submission_source_code,
source_system_code,
dw_last_update_date_time)
 SELECT
s.submission_sid,
s.valid_from_date,
s.valid_to_date,
s.submission_num,
s.last_modified_date,
s.new_submission_sw,
s.candidate_sid,
s.recruitment_requisition_sid,
s.candidate_profile_sid,
s.current_submission_status_id,
s.current_submission_step_id,
s.current_submission_workflow_id,
s.requisition_num,
s.job_application_num,
s.candidate_num,
s.matched_from_requisition_num,
CASE
	WHEN upper(s.source_system_code) = 'B' AND s.matched_from_requisition_num = 1 THEN 'Y'
	WHEN UPPER(s.source_system_code) = 'T' AND UPPER(rpm.profile_medium_desc) = UPPER('Matched to Job') THEN 'Y'
	ELSE 'N'
END AS Matched_Candidate_Flag,
s.submission_source_code,
s.source_system_code,
s.dw_last_update_date_time
FROM {{ params.param_hr_core_dataset_name }}.submission s
LEFT JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile cp
  ON s.candidate_profile_sid = cp.candidate_profile_sid
  AND cp.valid_to_date = '9999-12-31'
LEFT JOIN {{ params.param_hr_base_views_dataset_name }}.ref_profile_medium rpm
  ON cp.profile_medium_id = rpm .profile_medium_id
  and UPPER(rpm.source_system_code) = 'T';

ALTER TABLE {{ params.param_hr_core_dataset_name }}.submission
	RENAME TO submission_old;
ALTER TABLE {{ params.param_hr_core_dataset_name }}.submission_nt
	RENAME TO submission;

create or replace view `{{ params.param_hr_base_views_dataset_name }}.submission`
AS SELECT
    submission.submission_sid,
    submission.valid_from_date,
    submission.valid_to_date,
    submission.submission_num,
    submission.last_modified_date,
    submission.new_submission_sw,
    submission.candidate_sid,
    submission.recruitment_requisition_sid,
    submission.candidate_profile_sid,
    submission.current_submission_status_id,
    submission.current_submission_step_id,
    submission.current_submission_workflow_id,
    submission.requisition_num,
    submission.job_application_num,
    submission.candidate_num,
    submission.matched_from_requisition_num,
    submission.matched_candidate_flag,
    submission.submission_source_code,
    submission.source_system_code,
    submission.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.submission;
/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.submission AS SELECT
      a.submission_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.submission_num,
      a.last_modified_date,
      a.new_submission_sw,
      a.candidate_sid,
      a.recruitment_requisition_sid,
      a.candidate_profile_sid,
      a.current_submission_status_id,
      a.current_submission_step_id,
      a.current_submission_workflow_id,
      a.requisition_num,
      a.job_application_num,
      a.candidate_num,
      a.matched_from_requisition_num,
      a.matched_candidate_flag,
      a.submission_source_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.submission AS a
  ;

