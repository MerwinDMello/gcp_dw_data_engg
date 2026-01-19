drop view `{{ params.param_hr_bi_views_dataset_name }}.factsubmissionworkflow`;


create table if not exists `{{ params.param_hr_core_dataset_name }}.fact_submission_2248`
(
  submission_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each submission "),
  candidate_profile_num NUMERIC OPTIONS(description="A unique identifier for each candidate profile from the source."),
  candidate_num INT64 OPTIONS(description="The unique number for a candidate coming from the source."),
  candidate_profile_sid INT64 OPTIONS(description="A unique identifier for each candidate profile."),
  candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate."),
  submission_uid STRING OPTIONS(description="A combination of Coid and the cost center dept_sid or dept_code."),
  dept_sid INT64 OPTIONS(description="Unique ETL Generated Numeric number for each department code of Process level code associated with an HR Company Code."),
  company_code STRING OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes."),
  coid STRING OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities."),
  coid_dept_uid STRING OPTIONS(description="A combination of the dept_sid for the cost center and the coid"),
  creation_date DATE OPTIONS(description="Date the submission profile was created by the candidate."),
  creation_date_time DATETIME OPTIONS(description="Date time the submission profile was created by the candidate."),
  completion_date DATE OPTIONS(description="Date the submission profile was completed by the candidate."),
  completion_date_time DATETIME OPTIONS(description="Date time the submission profile was completed by the candidate."),
  recruitment_source_desc STRING OPTIONS(description="A description of the recruiting source. Ex. internet, job board, etc."),
  recruitment_source_type_desc STRING OPTIONS(description="A description of the recruiting source type."),
  recruitment_source_auto_filled_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  profile_medium_desc STRING OPTIONS(description="A description of the medium in which a profile was submitted to HCA."),
  recruitment_requisition_num_text STRING OPTIONS(description="A unique value coming from the source that combines the location code and Lawson requisition number."),
  recruitment_requisition_sid INT64 OPTIONS(description="A unique identifier for each requisition coming from the recruitment system."),
  lawson_requisition_num INT64 OPTIONS(description="A unique identifier for the Lawson requisition from Lawson."),
  ghr_requisition_num INT64 OPTIONS(description="The requisition_num for GHR records"),
  matched_from_requisition_num INT64 OPTIONS(description="Contains the requisition number that a candidate originally applied to as recruiters can sometimes move applications to other requisitions."),
  requisition_sid INT64 OPTIONS(description="Unique Id of an Lawson generated requisition."),
  status_code STRING OPTIONS(description="This represents the available code values for the status of the employee or applicant. "),
  first_offer_start_date DATE OPTIONS(description="This is the offer start date from the earliest valid record in the offer table."),
  open_fte_percent NUMERIC OPTIONS(description="FTE percent of opened requisition"),
  job_schedule_desc STRING OPTIONS(description="The description for the schedule of a job."),
  paid_source_flag STRING OPTIONS(description="An indicator that describes if the source of the submission was from a paid service."),
  last_modified_date DATE OPTIONS(description="Date the submission was last modified."),
  submission_status_name STRING OPTIONS(description="A name for each status of the submission process."),
  current_submission_step_name STRING OPTIONS(description="The spelled out name of the step within the submission process"),
  quality_flag STRING OPTIONS(description="If the Step Name is not New, New Applicant, or HR Reviews, then Yes. Everything else will be a No."),
  motive_name STRING OPTIONS(description="A name for each motive an event can have in the submission process."),
  new_grad_flag STRING OPTIONS(description="Y if the candidate is a registered nurse recent graduate."),
  gender_name STRING OPTIONS(description="Gender of the candidate spelled out"),
  ethnicity_name STRING OPTIONS(description="Ethnicity of the candidate as specified in the application process."),
  veteran_desc STRING OPTIONS(description="Describes whether the candidate is a veteran, and if so, what kind."),
  veteran_spouse_flag STRING OPTIONS(description="Indicates if their spouse is an active duty member of the military or not."),
  disability_flag STRING OPTIONS(description="Indicates whether the candidate has a disability of some kind or not."),
  hrr_duration_cnt INT64 OPTIONS(description="The number of days for HR to review submission"),
  hmr_duration_cnt INT64 OPTIONS(description="Total number of days for hiring manager to review submission"),
  interview_duration_cnt INT64 OPTIONS(description="Total number of days for interview process"),
  offer_duration_cnt INT64 OPTIONS(description="Total number of days to receive the offer"),
  matched_candidate_flag STRING OPTIONS(description="Indicates whether a candidate is a match"),
  candidate_intl_ext_flag STRING OPTIONS(description="Indicates whether the candidate is internal or external or unknown"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_sid
OPTIONS(
  description="Contains all the information specific to a single application submitted by a candidate.  Candidates can have multiple submissions, but each row on this table should provide the details about one only one of them."
);
INSERT INTO `{{ params.param_hr_core_dataset_name }}.fact_submission_2248`
    SELECT
  submission_sid,
  candidate_profile_num,
  candidate_num,
  candidate_profile_sid,
  candidate_sid,
  submission_uid,
  dept_sid,
  company_code,
  coid,
  coid_dept_uid,
  creation_date,
  null as creation_date_time,
  completion_date,
  null as completion_date_time,
  recruitment_source_desc,
  recruitment_source_type_desc,
  cast(recruitment_source_auto_filled_sw as INT64),
  profile_medium_desc,
  recruitment_requisition_num_text,
  recruitment_requisition_sid,
  lawson_requisition_num,
  null as ghr_requisition_num,
  matched_from_requisition_num,
  requisition_sid,
  status_code,
  null as first_offer_start_date,
  open_fte_percent,
  job_schedule_desc,
  paid_source_flag,
  last_modified_date,
  submission_status_name,
  current_submission_step_name,
  quality_flag,
  motive_name,
  new_grad_flag,
  gender_name,
  ethnicity_name,
  veteran_desc,
  veteran_spouse_flag,
  disability_flag,
  hrr_duration_cnt,
  hmr_duration_cnt,
  interview_duration_cnt,
  offer_duration_cnt,
  matched_candidate_flag,
  candidate_intl_ext_flag,
  source_system_code,
  dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.fact_submission;

ALTER TABLE `{{ params.param_hr_core_dataset_name }}.fact_submission`
RENAME TO `fact_submission_pre_2248`;
ALTER TABLE `{{ params.param_hr_core_dataset_name }}.fact_submission_2248`
RENAME TO `fact_submission`;

CREATE OR REPLACE VIEW
  {{ params.param_hr_base_views_dataset_name }}.fact_submission AS
SELECT
  submission_sid,
  candidate_profile_num,
  candidate_num,
  candidate_profile_sid,
  candidate_sid,
  submission_uid,
  dept_sid,
  company_code,
  coid,
  coid_dept_uid,
  creation_date,
  creation_date_time,
  completion_date,
  completion_date_time,
  recruitment_source_desc,
  recruitment_source_type_desc,
  recruitment_source_auto_filled_sw,
  profile_medium_desc,
  recruitment_requisition_num_text,
  recruitment_requisition_sid,
  lawson_requisition_num,
  ghr_requisition_num,
  matched_from_requisition_num,
  requisition_sid,
  status_code,
  first_offer_start_date,
  open_fte_percent,
  job_schedule_desc,
  paid_source_flag,
  last_modified_date,
  submission_status_name,
  current_submission_step_name,
  quality_flag,
  motive_name,
  new_grad_flag,
  gender_name,
  ethnicity_name,
  veteran_desc,
  veteran_spouse_flag,
  disability_flag,
  hrr_duration_cnt,
  hmr_duration_cnt,
  interview_duration_cnt,
  offer_duration_cnt,
  matched_candidate_flag,
  candidate_intl_ext_flag,
  source_system_code,
  dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.fact_submission;

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factsubmissions AS SELECT
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    01/06/2020
    --  Update:  1/10/20 to added Submission UID
    -- Update:  2/11/20 to add Submission Medium Desc and Offer_Grad Flag
    --  Update 3/24/20 to add the StepStatus_UID,
    --  Update: 4/3/20  replace EmpStatus Join with Job Schedule
    --  Update:  7/22/20 to add EEO info for Candidate
    -- -Update:  9/24/20  Use EDWHR_VIEWS.Ref_Submission_Step   Step_Short_Name in place of  Step_name for InforATS.  Adjust the Submission Status to reflect matching values from the desc field.
    -- -Update:  9/24/20  Edit EEO Joins to include field  InforATS populating
    -- -Update 10/26/20 to add Paid_Source Flag
    -- Update 3/25/2021 to count review,interview,offer time per person
    -- -Update 04/14/21 to sum the row level RIO data rather than just min/max to address those with steps out of order.
    -- -Updated 4/20/21  Modify qualify to look for the recruitment req with the offer rather than by source system
    -- -Updated 5/3/21 to add the New Grad Flag for ATS
    -- -Updated to include addional Ethnicity value location, gender update, and change to filter.
    -- - Updated 06/09/21 to remove case statement around Submission Status Name, Improve join to Offer table, and add FTE Value
    -- -Updated 4/6/22 to add the EEO values from new source
    -- --Updated 5/11/22 to include new Veteran status joins/coalesce
    -- - Updated 11/28/2022 to shift source to physicalized table created by Noah Lamborn, ITG  HDM-2116
    --  Updated 01/03/2023 added Matched_from_requisition_num
    --  Description: FactSubmission
    -- Metrics:  # of applications, rejected candiate motive, sourcing
    --  =============================================
    s.candidate_profile_num AS taleo_submission_id,
    s.submission_sid,
    s.candidate_num AS taleo_candidate_id,
    s.candidate_profile_sid,
    s.candidate_sid,
    s.submission_uid,
    s.dept_sid,
    s.coid,
    s.coid_dept_uid AS coid_uid,
    last_day(DATE(s.creation_date)) AS pe_date,
    s.creation_date AS submission_create_date,
    EXTRACT(TIME FROM s.creation_date_time) AS submission_create_time,
    s.completion_date AS submission_completed_date,
    EXTRACT(TIME FROM s.completion_date_time) AS submission_completed_time,
    CASE
      WHEN s.completion_date IS NULL THEN 0
      ELSE 1
    END AS iscompleted,
    s.recruitment_source_desc AS source_desc,
    s.recruitment_source_type_desc AS source_type,
    s.recruitment_source_auto_filled_sw AS source_autofill,
    s.profile_medium_desc AS submission_medium,
    s.recruitment_requisition_num_text AS taleo_requisition_num,
    s.recruitment_requisition_sid,
    s.lawson_requisition_num,
    s.ghr_requisition_num,
    s.matched_from_requisition_num,
    s.requisition_sid,
    s.status_code AS emp_status,
    s.open_fte_percent AS fte_value,
    s.job_schedule_desc,
    s.paid_source_flag AS paid_source,
    s.last_modified_date,
    s.submission_status_name AS current_submission_status,
    s.current_submission_step_name AS current_submission_step,
    s.quality_flag AS quality_ind,
    s.motive_name AS rejection_reason,
    1 AS sub_measure_qty,
    s.new_grad_flag AS offer_grad_flag,
    s.gender_name,
    s.ethnicity_name,
    s.veteran_desc,
    s.veteran_spouse_flag,
    s.disability_flag AS disability_ind,
    s.hrr_duration_cnt AS hrr_duration,
    s.hmr_duration_cnt AS hmr_duration,
    s.interview_duration_cnt AS interview_duration,
    s.offer_duration_cnt AS offer_duration,
    s.matched_candidate_flag AS matched_candidate,
    s.candidate_intl_ext_flag AS candidate_ie_ind,
    coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
    s.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_submission s
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department dept
      ON s.dept_sid = dept.dept_sid
      AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department sf
      ON LEFT(dept.dept_code, 3) = sf.dept_num
      AND s.coid = sf.coid
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department df
      ON sf.functional_dept_num = df.functional_dept_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility ff
      ON s.coid = ff.coid

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat1  --Process Level AND Dept Num
      ON Left(s.recruitment_requisition_num_text, 5) = mat1.process_level_code 
      AND dept.dept_code = mat1.dept_code
      AND mat1.match_level_num = 1

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat2  --LOB and Sub LOB
      ON ff.lob_code = mat2.lob_code 
      AND ff.sub_lob_code = mat2.sub_lob_code
      AND mat2.match_level_num = 2

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat3  --Function and Sub Function
      ON sf.functional_dept_desc = mat3.functional_dept_desc
      AND sf.sub_functional_dept_desc = mat3.sub_functional_dept_desc
      AND mat3.match_level_num = 3

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat4  --Process Level
      ON Left(s.recruitment_requisition_num_text, 5) = mat4.process_level_code
      AND mat4.match_level_num = 4
;


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
