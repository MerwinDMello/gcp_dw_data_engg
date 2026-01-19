CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.personnel_time`
(
  employee_num INT64 NOT NULL OPTIONS(description="This is the lawson employee number"),
  process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value maintained in this field."),
  clock_library_code STRING NOT NULL OPTIONS(description="Code for each clock library."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically."),
  valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated."),
  personnel_name STRING OPTIONS(description="Name of the personnel."),
  hire_date_time DATETIME OPTIONS(description="The hire date and time of the personnel."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="The number that identifies a company. A company represents a business or legal entity of an organization"),
  job_code STRING OPTIONS(description="Unique Job code of an HR Company."),
  dept_code STRING OPTIONS(description="Unique Department code of an HR Company"),
  pay_type_code STRING OPTIONS(description="Describes which Kronos pay code an employee is assigned to."),
  termination_date DATE OPTIONS(description="Date on which employee is terminated from an HR company"),
  employee_34_login_code STRING OPTIONS(description="The User Id field will be populated from the actual system user ID code when the user adds, changes, or deletes a record."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_num, process_level_code, clock_library_code
OPTIONS(
  description="This tables contains all hourly employees (including contractors)."
);