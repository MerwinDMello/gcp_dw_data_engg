create table if not exists `{{ params.param_hr_core_dataset_name }}.employee`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  process_level_sid INT64 NOT NULL OPTIONS(description="unique id generated for process level code of an hr company"),
  dept_sid INT64 NOT NULL OPTIONS(description="unique etl generated numeric number for each department code of process level code associated with an hr company code."),
  location_code STRING OPTIONS(description="unique location codes of an employee working location facility."),
  pay_grade_code STRING NOT NULL OPTIONS(description="pay grade of an position"),
  union_code STRING NOT NULL OPTIONS(description="it captured union code of an employee. hca staff is associated with union like nurse union, maintenance union and each union is identified using its unique code. "),
  user_level_code STRING NOT NULL OPTIONS(description="it captures employee user level code. user level code provides information about employee that he is associated with urgent care or emergency unit etc."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  employee_num INT64 NOT NULL OPTIONS(description="this is an lawson employee number"),
  account_unit_num STRING OPTIONS(description="this field  maintains employee home general ledger accounting unit for expense"),
  gl_company_num INT64 OPTIONS(description="this field maintains employees home general  ledger company for expense"),
  dept_code STRING NOT NULL OPTIONS(description="unique department code associated for an hr company & process level captured in this field"),
  employee_34_login_code STRING OPTIONS(description="the user id field will be populated from the actual system user id code when the user adds, changes, or deletes a record."),
  adjusted_hire_date DATE OPTIONS(description="hire date used to determine pto accrual"),
  anniversary_date DATE OPTIONS(description="contains employees anniversary date of employment"),
  termination_date DATE OPTIONS(description="date on which employee is terminated from an hr company"),
  hire_date DATE OPTIONS(description="date employee was hired"),
  new_hire_date DATE OPTIONS(description="the date the employee was reported to the state as a new hire."),
  primary_facility_ind STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  fte_percent NUMERIC OPTIONS(description="this field maintains full time employee percent allocated for an hr company. for example if fte value > 0.7 it means employee is full time. if fte value < 0.7 then employee is part time or temporary"),
  pay_grade_schedule_code STRING NOT NULL OPTIONS(description="unique schedule code defined for pay grade and pay step"),
  pay_step_num INT64 NOT NULL OPTIONS(description="contains the step if this is a step and grade schedule. for grade range schedules, step 1 is the minimum, step 2 is the midpoint, and step 3 is the maximum pay rate for the grade."),
  pay_rate_amt NUMERIC NOT NULL OPTIONS(description="contains the pay rate from the employee file if using step and grade. if using a grade range schedule, this is either the minimum, midpoint, or maximum."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  remote_sw INT64 OPTIONS(description="numeric boolean to where 0 = false/no and 1 = true/yes"),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid
OPTIONS(
  description="contains all employee information needed in the lawson payroll system."
);