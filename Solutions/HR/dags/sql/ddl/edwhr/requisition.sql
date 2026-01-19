create table if not exists `{{ params.param_hr_core_dataset_name }}.requisition`
(
  requisition_sid INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  hr_company_sid INT64 OPTIONS(description="etl generated unique number for an unique hr company number"),
  application_status_sid INT64 OPTIONS(description="unique identifier generated for values of an status code"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="unique code of an hr company facility or process level. this field is spaces for hr companyrecords"),
  location_code STRING NOT NULL OPTIONS(description="unique location codes of an employee working location facility."),
  requisition_num INT64 NOT NULL OPTIONS(description="unique id of an lawson generated requisition."),
  requisition_desc STRING OPTIONS(description="description of an requisition"),
  requisition_eff_date DATE OPTIONS(description="date on which the requisition becomes effective."),
  requisition_open_date DATE OPTIONS(description="date on which requisition is opened"),
  requisition_closed_date DATE OPTIONS(description="date on which requisition is closed"),
  requisition_origination_date DATE OPTIONS(description="date the requisition was originated by the orginator."),
  originator_login_3_4_code STRING OPTIONS(description="the employee number of the originator of the requisition."),
  position_needed_date DATE OPTIONS(description="date the position is needed to be loaded."),
  job_opening_cnt INT64 OPTIONS(description="contains the number of open jobs or positions of an hr company"),
  open_fte_percent NUMERIC OPTIONS(description="fte percent of opened requisition"),
  filled_fte_percent NUMERIC OPTIONS(description="reqyuisition filled fte percent"),
  last_update_date DATE OPTIONS(description="date that the last update occured."),
  replacement_employee_num INT64 OPTIONS(description="id of the employee performing the job duties of the requisition while it is open."),
  replacement_employee_sid INT64 OPTIONS(description="etl generated unique number for the employee performing the job duties of the requisition while it is open."),
  replacement_flag STRING OPTIONS(description="this flag identifies if the employee is a replacement or not."),
  special_requirement_text STRING OPTIONS(description="description of the special requirement for this position like skills and qualification."),
  work_schedule_code STRING OPTIONS(description="code that describes the weekly schedule of the requisition."),
  union_code STRING OPTIONS(description="it captures union code of an employee. hca staff is associated with union like nurse union, maintenance union and each union is identified using its unique code. "),
  active_dw_ind STRING NOT NULL OPTIONS(description="y/n character to indicate this record as active in the edw."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY requisition_sid
OPTIONS(
  description="requisition opened against position of an hr company captured in this table"
);