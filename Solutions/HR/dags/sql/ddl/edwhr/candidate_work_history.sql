CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_work_history
(
  candidate_work_history_sid INT64 NOT NULL OPTIONS(description="A unique identifier that tracks work history of a candidate on a submission."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded."),
  valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded."),
  candidate_work_history_num INT64 OPTIONS(description="A unique number from the source that tracks work history of a candidate on a submission."),
  candidate_profile_sid INT64 OPTIONS(description="A unique identifier for each candidate profile."),
  candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate."),
  work_start_date DATE OPTIONS(description="The date the candidate started working at a previous employer."),
  work_end_date DATE OPTIONS(description="The date the candidate ended working at a previous employer."),
  current_employer_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  profile_display_seq_num INT64 OPTIONS(description="This represents the order of work history within the application profile of a candidate."),
  employer_name STRING OPTIONS(description="Name of the employer of the candidate."),
  job_title_name STRING OPTIONS(description="The title of the candidates previous job."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_work_history_sid
OPTIONS(
  description="Contains details associated with the work history of a candidate."
);