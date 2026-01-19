create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_position_security_role`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique etl generated number for an hca employee number"),
  position_sid INT64 NOT NULL OPTIONS(description="unique etl generated sequence number for each user defined codes that represents a position in the hr company."),
  eff_from_date DATE NOT NULL OPTIONS(description="effective start date of a record"),
  span_code STRING OPTIONS(description="category of security access an employee is provisioned to."),
  access_role_code STRING OPTIONS(description="describes the role of the user."),
  job_code STRING OPTIONS(description="this is the job code for the job requisition."),
  employee_num INT64 OPTIONS(description="this is an lawson employee number"),
  coid STRING OPTIONS(description="the company identifier which uniquely identifies a facility to corporate and all other facilities."),
  company_code STRING OPTIONS(description="part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes."),
  dept_code STRING OPTIONS(description="this is the lawson department number for the requisition"),
  position_code STRING OPTIONS(description="this field captures position code of an employee"),
  employee_34_login_code STRING OPTIONS(description="the user id field will be populated from the actual system user id code when the user adds, changes, or deletes a record."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="this field contains value of an process level code of an company"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY eff_from_date
CLUSTER BY employee_sid, position_sid
OPTIONS(
  description="this table matches security data from security_role_detail to hr data. it assigns a span code to each position."
);