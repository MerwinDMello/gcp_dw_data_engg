CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.employee_work_history
(
  employee_work_history_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each employee history record."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Start Date of the record"),
  employee_talent_profile_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  previous_work_address_sid INT64 OPTIONS(description="This field maintains unique sequence number for each employee number"),
  work_history_company_name STRING OPTIONS(description="Company name for employees work history"),
  work_history_job_title_text STRING OPTIONS(description="Job title for employees work history"),
  work_history_desc STRING OPTIONS(description="Description of responsibilities for employees work history"),
  work_history_start_date DATE OPTIONS(description="Start date for employees work history"),
  work_history_end_date DATE OPTIONS(description="End date for employees work history"),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  valid_to_date DATETIME OPTIONS(description="End Date of the record"),
  source_system_key STRING OPTIONS(description="A unique key coming from the source."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_work_history_sid, valid_from_date
OPTIONS(description="This table maintains employee entire previous organization history");