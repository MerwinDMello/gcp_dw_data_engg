create table if not exists `{{ params.param_hr_core_dataset_name }}.person_action`
(
  person_action_sid INT64 NOT NULL OPTIONS(description="etl generated unique identifier for an combination of fields action code, company employee identifier, action sequence number & action effective date"),
  eff_from_date DATE NOT NULL OPTIONS(description="date on which record is initiated"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  action_code STRING NOT NULL OPTIONS(description="contains the action type associated with the personnel action"),
  employee_sid INT64 OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  applicant_sid INT64 OPTIONS(description="it is the surrogate key generated based on applicant number and company number"),
  employee_num INT64 OPTIONS(description="it is the employee number of an employee"),
  applicant_num INT64 OPTIONS(description="it is the applicant number provided to person who applied for an job in hr company"),
  action_type_code STRING OPTIONS(description="it represents the various action type code values availabe for personnel action. valid values are as follow  a = hire an applicant,e = individual action,m = mass change,p = mass pay change,l = multiple positions"),
  action_sequence_num INT64 NOT NULL OPTIONS(description="contains the internal action number, which is automatically assigned in lawson"),
  action_from_date DATE NOT NULL OPTIONS(description="date on which action is executed"),
  action_to_date DATE OPTIONS(description="date on which action is  expected to end"),
  requisition_sid INT64 OPTIONS(description="unique id of an lawson generated requisition."),
  action_reason_text STRING OPTIONS(description="reason description of an action taken on employee"),
  person_action_update_sid INT64 OPTIONS(description="employee who has updated the action"),
  person_action_flag STRING OPTIONS(description="this flag identifies if action is approved or pending."),
  action_last_update_date DATE OPTIONS(description="date on which action is updated in lawson system"),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY person_action_sid, valid_from_date
OPTIONS(
  description="employee of an hr company goes through various actions like pay hike, termination etc over the period of time and this table maintains current action performed on employee and its history."
);