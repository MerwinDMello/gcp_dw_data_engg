create table if not exists `{{ params.param_hr_core_dataset_name }}.hr_employee_history`
(
  employee_sid INT64 NOT NULL OPTIONS(description="system generated unique value for each combination of employee number and lawson company number."),
  lawson_element_num INT64 NOT NULL OPTIONS(description="lawson dictionary number of the data item  being logged."),
  hr_employee_element_date DATE NOT NULL OPTIONS(description="date for which the recorded data item value became effective"),
  lawson_object_id INT64 NOT NULL OPTIONS(description="if the item being logged is from an employee deduction, standard time record, job code, or supervisor this        field will contain the object id of that  record."),
  sequence_num INT64 NOT NULL OPTIONS(description="if multiple records are created in lawson for the  same effective date field will be incremented for each  record for the date."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  hr_employee_value_num NUMERIC OPTIONS(description="if the data item is a numeric field  the value is stored in this field"),
  hr_employee_value_alphanumeric_text STRING OPTIONS(description="if the data item is alpha-numeric, the value is stored in this field."),
  hr_employee_value_date DATE OPTIONS(description="if the data item is a date, the value is stored in this field."),
  action_object_identifier INT64 OPTIONS(description="if the change was done via company employee action, the record identifier of that action  is stored here"),
  user_3_4_login_code STRING OPTIONS(description="the user id field will be populated from the actual system user id code when the user adds, changes, or deletes a record."),
  data_type_flag STRING OPTIONS(description="indicator of the type of data record  for which changes are logged. e = employee, a = applicant, d = deduction, j = job code, p = position ,s = supervisor, t = standard time record ,l = leave plan master"),
  position_level_sequence_num INT64 OPTIONS(description="primary or secondary position level of an employee"),
  last_update_date DATE OPTIONS(description="date that the last update occured."),
  last_update_time TIME OPTIONS(description="time that the last update occured."),
  employee_num INT64 OPTIONS(description="this is an lawson employee number"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_to_date)
CLUSTER BY employee_sid, lawson_element_num, lawson_object_id, sequence_num
OPTIONS(
  description="contains records consisting of the new value and a date stamp for each field in  the employee, the system logs any field changed by a personnel action"
);