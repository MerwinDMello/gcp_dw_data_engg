CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.employee_competency_detail
(
  employee_competency_result_sid INT64 NOT NULL OPTIONS(description="A unique number for each employee competency record."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Start date of the record"),
  employee_talent_profile_sid INT64 OPTIONS(description="Display a unique identifier for the employee within the employers HR system (Lawson)"),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  performance_plan_id INT64 OPTIONS(description="Displays the name of the performance management plan"),
  competency_group_id INT64 OPTIONS(description="A categorization of related competencies"),
  competency_id INT64 OPTIONS(description="Describes a key competency for the employees position"),
  evaluation_workflow_status_id INT64 OPTIONS(description="Indicates the current state of the evaluation workflow process for the employee"),
  review_period_id INT64 OPTIONS(description="Period for which the review was executed"),
  review_year_num INT64 OPTIONS(description="Year associated with the review period"),
  review_period_start_date DATE OPTIONS(description="Start date of the review period"),
  review_period_end_date DATE OPTIONS(description="End date of the review period"),
  employee_rating_num INT64 OPTIONS(description="Employees self rating of an individual competency represented in a numeric value"),
  employee_rating_id INT64 OPTIONS(description="Employees self rating of an individual competency represented in a scale value"),
  manager_rating_num INT64 OPTIONS(description="Manager rating of an employees individual competency represented in a numeric value"),
  manager_rating_id INT64 OPTIONS(description="Manager rating of an employees individual competency represented in a scale value"),
  manager_employee_rating_gap_num INT64 OPTIONS(description="Displays the difference between the employee rating and the manager rating"),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  valid_to_date DATETIME OPTIONS(description="End date of the record"),
  source_system_key STRING OPTIONS(description="Unique key coming from the source system."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_competency_result_sid
OPTIONS(description="Employee competency effectiveness proven information maintained in this table");