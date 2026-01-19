create table if not exists `{{ params.param_hr_core_dataset_name }}.fact_step_status`
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