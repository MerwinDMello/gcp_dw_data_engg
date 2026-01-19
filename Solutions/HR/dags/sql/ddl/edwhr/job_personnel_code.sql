create table if not exists `{{ params.param_hr_core_dataset_name }}.job_personnel_code`
(
  job_code_sid INT64 NOT NULL OPTIONS(description="unique job code of an hr company."),
  position_sid INT64 NOT NULL OPTIONS(description="unique etl generated sequence number for each user defined codes that represents a position in the hr company."),
  personnel_type_code STRING NOT NULL OPTIONS(description="code that describes the relationship type."),
  personnel_code STRING NOT NULL OPTIONS(description="contains certification, education, and skill codes."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="contains the unique identifer for the hr company associated with the history record"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  required_flag_ind STRING OPTIONS(description="indicates whether certain qualifications are needed for a job with y/n."),
  personnel_code_time_pct NUMERIC OPTIONS(description="contains the percent of the time that the employee will spend on this essential function."),
  proficiency_level_desc STRING OPTIONS(description="contains the proficiency level of the job."),
  weight_amt NUMERIC OPTIONS(description="describes weight of job qualifications."),
  subject_code STRING NOT NULL OPTIONS(description="contains subject codes for education records."),
  job_code STRING OPTIONS(description="unique job code of an hr company."),
  position_code STRING NOT NULL OPTIONS(description="this represents the user defined codes that represents a position in the hr company."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company. a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY job_code_sid, position_sid, personnel_type_code, personnel_code
OPTIONS(
  description="this table stores the required certifications for a specific job code or a position."
);