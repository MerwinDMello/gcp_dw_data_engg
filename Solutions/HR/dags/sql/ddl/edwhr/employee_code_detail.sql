create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_code_detail`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  employee_type_code STRING NOT NULL OPTIONS(description="identifies the type of code."),
  employee_sw INT64 NOT NULL OPTIONS(description="numeric boolean to where 0 = false/no and 1 = true/yes"),
  employee_code STRING NOT NULL OPTIONS(description="the code associated with the employee."),
  employee_code_subject_code STRING NOT NULL OPTIONS(description="the subject of the code."),
  employee_code_seq_num INT64 NOT NULL OPTIONS(description="the numeric sequence of the code for the same employee for hte same type of code."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  employee_num INT64 OPTIONS(description="this is an lawson employee number"),
  acquired_date DATE OPTIONS(description="date the code was acquired."),
  renew_date DATE OPTIONS(description="date the code was renewed."),
  certification_renew_date DATE OPTIONS(description="this is the renewal date for certifications."),
  license_num_text STRING OPTIONS(description="the license number associated with certain code types."),
  proficiency_level_text STRING OPTIONS(description="this field shows the proficiency level of the employee."),
  verified_ind STRING OPTIONS(description="indicates if the code/license have "),
  employee_code_detail_text STRING OPTIONS(description="contains detail aligned with the employee code like education institution, skill instructor, tuition reimbursement info, company property, etc."),
  company_sponsored_ind STRING OPTIONS(description="indicates whether the certification, education, or skill was company sponsored."),
  skill_source_code STRING OPTIONS(description="describes what group is responsible for the credentials."),
  lawson_company_num INT64 OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  process_level_code STRING OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  state_code STRING OPTIONS(description="the state code is the abbreviated 2 character code associated with the address state for a person or organization."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  active_dw_ind STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  delete_ind STRING OPTIONS(description="the indicator is usually a y/ n (yes/no) designation of a record but can sometimes be other values.  this should not be 1/0 designation."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid, employee_type_code, employee_sw, employee_code
OPTIONS(
  description="contains the codes associated with employee certification, education, skill, property and other types."
);