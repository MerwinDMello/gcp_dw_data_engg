create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_requisition_comment`
(
  employee_sid INT64 NOT NULL OPTIONS(description="system generated unique value for each combination of employee number and lawson company number."),
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  applicant_type_id INT64 NOT NULL OPTIONS(description="indicates if the individual is an employee or an applicant.lawson system maintains 0 and 1 for employee and applicant respectively and same values are maintained in this field."),
  comment_type_code STRING NOT NULL OPTIONS(description="contains the comment type code"),
  action_code STRING NOT NULL OPTIONS(description="contains the action code for personnel action comments. this value is blank for employee, applicant, and job description comments."),
  comment_line_num INT64 NOT NULL OPTIONS(description="contains the comment type number."),
  sequence_num INT64 NOT NULL OPTIONS(description="if multiple records are created in lawson for the  same effective date field will be incremented for each  record for the date."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="the number that identifies a hr company. hr company represents a business or legal                      entity of an organization as maintained in lawson. this column carries lawson defined unique hr company  value."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company. a company represents a business or legal entity of an organization"),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  comment_text STRING OPTIONS(description="text of the comment"),
  comment_date DATE OPTIONS(description="contains date the comment was entered."),
  print_code STRING OPTIONS(description="indicates whether the comment will print on reports."),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  requisition_num INT64 OPTIONS(description="lawson generated requisition number"),
  employee_num INT64 NOT NULL OPTIONS(description="lawson generated employee number"),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid, requisition_sid, applicant_type_id, comment_type_code
OPTIONS(
  description="table contains comments for employees or applicants"
);