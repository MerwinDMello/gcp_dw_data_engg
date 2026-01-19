create table if not exists `{{ params.param_hr_core_dataset_name }}.job_position`
(
  position_sid INT64 NOT NULL OPTIONS(description="unique etl generated sequence number for each user defined codes that represents a position in the hr company."),
  eff_from_date DATE NOT NULL OPTIONS(description="date on which record is initiated"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  link_supervisor_sid INT64 NOT NULL OPTIONS(description="this represents the supervisor system identifier that identifies the supervisor to whom the position is linked"),
  supervisor_sid INT64 NOT NULL OPTIONS(description="this field maintains supervisor system identifier that identifies the supervisor to whom the employee reports. "),
  job_code_sid INT64 NOT NULL OPTIONS(description="unique job code of an hr company."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="contains the unique identifer for the hr company associated with the history record"),
  position_change_reason_code STRING OPTIONS(description="the reason for the change to the position."),
  location_code STRING OPTIONS(description="unique location codes of an employee working location facility."),
  pay_grade_code STRING OPTIONS(description="pay grade of an position"),
  position_code STRING NOT NULL OPTIONS(description="this represents the user defined codes that represents a position in the hr company."),
  account_unit_num STRING OPTIONS(description="all expenses related to position are captured under account unit."),
  gl_company_num INT64 OPTIONS(description="this field maintains general ledger expense company"),
  overtime_plan_code STRING NOT NULL OPTIONS(description="this field captures overtime code of an position."),
  overtime_exempt_ind STRING OPTIONS(description="indicates if the position is exempt from overtime pay. values are  y = yes (salaried rate of pay), n = no (hourly rate of pay)  "),
  position_code_desc STRING NOT NULL OPTIONS(description="description of an position code of an hr company"),
  company_position_eff_from_date DATE OPTIONS(description="position effective start date value"),
  company_position_eff_to_date DATE OPTIONS(description="position effective end date value"),
  pay_grade_schedule_code STRING OPTIONS(description="pay grade or pay grade and step schedule code value"),
  pay_step_num INT64 OPTIONS(description="contains the step if this is a step and grade schedule. for grade range schedules, step 1 is the minimum, step 2 is the midpoint, and step 3 is the maximum pay rate for the grade."),
  union_code STRING OPTIONS(description="it is the union code to which employee is associated. "),
  shift_num INT64 OPTIONS(description="this field maintains the shift with which the  positions pay is associated."),
  eff_to_date DATE NOT NULL OPTIONS(description="expiry date of an record"),
  lawson_object_id INT64 OPTIONS(description="contains the unique identifier that ties the transaction back to lawson."),
  schedule_work_code STRING OPTIONS(description="employee scheduled work code value"),
  user_level_code STRING OPTIONS(description="this field maintains user level code of an employee. this is the unique code of an urgent care, emergency care etc"),
  dept_sid INT64 OPTIONS(description="unique etl generated numeric number for each department code of process level code associated with an hr company code."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY position_sid, eff_from_date, valid_from_date
OPTIONS(
  description="contains the overtime pay code. if the  pay plan is an overtime plan.flsa overtime calculation automatically calculates the overtime and creates a time record for the overtime pay code"
);