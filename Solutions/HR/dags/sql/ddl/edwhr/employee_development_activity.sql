CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.employee_development_activity
(
  employee_development_activity_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each employee development activity record."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Record effective start date"),
  employee_talent_profile_sid INT64 OPTIONS(description="It is the ETL generated "),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  development_activity_name STRING OPTIONS(description="Displays the name of an activity selected for the employees development plan"),
  development_activity_desc STRING OPTIONS(description="Description of the activity"),
  catalog_activity_name STRING OPTIONS(description="The name of the catalog activity."),
  catalog_activity_desc STRING OPTIONS(description="A long description of the catalog activity."),
  development_activity_status_id INT64 OPTIONS(description="A unique identifier that indicates the status of the development activity."),
  development_activity_priority_id INT64 OPTIONS(description="Indicates the priority of the development activity. This field maintains 4 flag of which H Indicates High Priority, M indicates Medium Priority & L indicates Low Priority."),
  development_activity_start_date DATE OPTIONS(description="Displays the starting date for the development activity"),
  development_activity_end_date DATE OPTIONS(description="Displays the ending date for the development activity"),
  development_activity_hour_text STRING OPTIONS(description="Displays the hours the employee spent on the development activity. This is the free text allowed to be entered by employee"),
  development_activity_comment_text STRING OPTIONS(description="Displays any notes recorded about the activity (eg.  progress, further explanation)"),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  valid_to_date DATETIME OPTIONS(description="Record Effective End date"),
  source_system_key STRING OPTIONS(description="A unique key coming from the source."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_development_activity_sid
OPTIONS(description="Employee selfe decided or organisation provided development activities are maintained in this table");
