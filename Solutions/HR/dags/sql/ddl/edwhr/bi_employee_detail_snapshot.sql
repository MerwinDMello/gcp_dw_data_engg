create table if not exists `{{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot`
(
  employee_sid INT64 NOT NULL OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  snapshot_date DATE NOT NULL OPTIONS(description="Date the snapshot was taken."),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  employee_first_name STRING OPTIONS(description="This field maintain employee first name"),
  employee_last_name STRING OPTIONS(description="It captures employees last name"),
  employee_middle_name STRING OPTIONS(description="It defines middle name of the employee"),
  ethnic_origin_code STRING OPTIONS(description="It defines employee ethnic origin code"),
  gender_code STRING OPTIONS(description="Unique Gender code of an employee maintains in this field. F = Female M = Male "),
  adjusted_hire_date DATE OPTIONS(description="Hire date used to determine PTO accrual"),
  birth_date DATE OPTIONS(description="The date of birth of the candidate."),
  acute_experience_start_date DATE OPTIONS(description="Start date for a nurse with acute experience."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY snapshot_date
CLUSTER BY employee_sid, snapshot_date
OPTIONS(
  description="Snapshot of employee information used for reporting purposes."
);