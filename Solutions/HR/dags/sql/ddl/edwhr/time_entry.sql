CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.time_entry`
(
  employee_num INT64 NOT NULL OPTIONS(description="This is the lawson employee number"),
  kronos_num NUMERIC(29) NOT NULL OPTIONS(description="Date DATETIME for the employees clock in."),
  clock_library_code STRING NOT NULL OPTIONS(description="Code for each clock library."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically."),
  valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated."),
  clock_code STRING OPTIONS(description="Unique Id for each clock."),
  clock_in_time DATETIME OPTIONS(description="Time that the employee clocked in."),
  clock_out_time DATETIME OPTIONS(description="Time that the employee clocked out."),
  clocked_hour_num NUMERIC(32, 3) OPTIONS(description="Amount of time the employee was clocked in for."),
  rounded_clock_in_time DATETIME OPTIONS(description="Employees time clocked in rounded to the nearest 15 minute increment."),
  rounded_clock_out_time DATETIME OPTIONS(description="Employees time clocked out rounded to the nearest 15 minute increment."),
  rounded_clocked_hour_num NUMERIC(32, 3) OPTIONS(description="Total of employees time clocked in rounded to the nearest 15 minute increment."),
  time_approval_date_time DATETIME OPTIONS(description="Date and time that the employees time was approved."),
  time_approver_34_login_code STRING OPTIONS(description="3/4 of the person who approved the employees time."),
  scheduled_shift_date_time DATETIME OPTIONS(description="Date and time of the employees scheduled shift."),
  pay_period_start_date_time DATETIME OPTIONS(description="The end date time of the pay period associated with the time punch."),
  pay_period_end_date_time DATETIME OPTIONS(description="The start date time of the pay period associated with the time punch."),
  pay_type_code STRING OPTIONS(description="Describes which Kronos pay code an employee is assigned to."),
  long_meal_code STRING OPTIONS(description="Code that designates an exception for long meal in Kronos. N = no exception, I = in exception"),
  other_dept_code STRING OPTIONS(description="Code that designates an exception for other department in Kronos. N = no exception, I = in exception"),
  out_of_pay_period_code STRING OPTIONS(description="Code that designates an exception for out of pay period meal in Kronos. N = no exception, I = in exception"),
  short_meal_code STRING OPTIONS(description="Code that designates an exception for short meal in Kronos. N = no exception, I = in exception"),
  dept_code STRING OPTIONS(description="Unique Department code of an HR Company"),
  posted_ind STRING OPTIONS(description="Designates whether a time record has been posted to payroll or not."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="The number that identifies a company. A company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="DATETIME of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_num, kronos_num, clock_library_code
OPTIONS(
  description="Contains information about each employees time record"
);