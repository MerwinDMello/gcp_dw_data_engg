CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.fact_employee_availability
(
  employee_talent_profile_sid INT64 NOT NULL OPTIONS(description="This is the unique sequence number generated for each employee number"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="record effective start date"),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  valid_to_date DATETIME OPTIONS(description="Record effective end date"),
  jobs_pooled_for_cnt INT64 OPTIONS(description="Number of jobs that the employee is currently pooled for"),
  employee_talent_pool_cnt INT64 OPTIONS(description="Displays the number of people currently in the talent pool for the position, for positions that allow succession slating"),
  employee_successor_cnt INT64 OPTIONS(description="Displays the number of people currently on the succession slate for the position, for positions that allow succession slating"),
  employee_ready_now_cnt INT64 OPTIONS(description="An aggregate count of the number of employees who have been designated as immediate ready successors for this position."),
  employee_ready_18_24_month_cnt INT64 OPTIONS(description="An aggregate count of the number of employees who have been designated with a readiness timeframe of 18-24 months as successors for this position."),
  employee_ready_12_18_month_cnt INT64 OPTIONS(description="An aggregate count of the number of employees who have been designated with a readiness timeframe of 12-18 months as successors for this position."),
  employee_ready_6_11_month_cnt INT64 OPTIONS(description="An aggregate count of the number of employees who have been designated with a readiness timeframe of 6-11 months as successors for this position."),
  employee_other_readiness_cnt INT64 OPTIONS(description="An aggregate count of the number of employees who have been designated with an unknown readiness timeframe as a successor for this position"),
  employee_readiness_unknown_cnt INT64 OPTIONS(description="An aggregate count of the number of employees whose readiness as successors for this position is unknown"),
  employee_slated_for_position_cnt INT64 OPTIONS(description="Number of positions that the employee is slated for"),
  employee_talent_pooled_for_position_cnt INT64 OPTIONS(description="Number of positions that the employee is in the talent pool for."),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY employee_talent_profile_sid
OPTIONS(description="This table maintains employee readiness for another role and also similar role employee are in line for next level role.");