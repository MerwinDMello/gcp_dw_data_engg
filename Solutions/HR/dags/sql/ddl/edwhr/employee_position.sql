create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_position`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  position_sid INT64 NOT NULL OPTIONS(description="this represents the user defined codes that represents a position in the hr company."),
  position_level_sequence_num INT64 NOT NULL OPTIONS(description="it is represents primary or secondary position of an employee"),
  eff_from_date DATE NOT NULL OPTIONS(description="date on which record is initiated"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  eff_to_date DATE NOT NULL OPTIONS(description="expiry date of an record"),
  fte_percent NUMERIC OPTIONS(description="this field maintains full time employee percent allocated for an hr company. for example if fte value > 0.7 it mena employee is full time. if fte value < 0.7 then employee is part time or temporary"),
  working_location_code STRING OPTIONS(description="employee physical working location code maintained in this field"),
  schedule_work_code STRING OPTIONS(description="employee scheduled work code value"),
  job_code STRING OPTIONS(description="unique job code of an hr company."),
  employee_num INT64 OPTIONS(description="this is an lawson employee number"),
  pay_rate_amt NUMERIC OPTIONS(description="hourly pay rate amount as per pay grade and step"),
  last_update_date DATE OPTIONS(description="date that the last update occured."),
  dept_sid INT64 OPTIONS(description="unique etl generated numeric number for each department code of process level code associated with an hr company code."),
  account_unit_num STRING OPTIONS(description="all expenses related to position are captured under account unit."),
  gl_company_num INT64 OPTIONS(description="this field maintains general ledger expense company"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_sid, position_sid, position_level_sequence_num, valid_from_date
OPTIONS(
  description="each employee is assigned with an position code and its level while associated with an hr company. position is attributed by information like full time employee percentage of working (fte percent), working location ,schedule work,pay rate amount etc. "
);