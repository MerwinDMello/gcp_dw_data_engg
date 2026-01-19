CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.employee_goal_detail
(
  employee_goal_detail_sid INT64 NOT NULL OPTIONS(description="A unique number for each record of an employees goal details."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Start date of the record"),
  employee_talent_profile_sid INT64 OPTIONS(description="Display a unique identifier for the employee within the employers HR system (Lawson)"),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  employee_goal_year_num INT64 OPTIONS(description="Year associated with the review period"),
  goal_name STRING OPTIONS(description="Describes a goal included in an employees evaluation.  This is the name of the goal."),
  goal_category_id INT64 OPTIONS(description="Displays the category by which the goal is grouped within the plan"),
  goal_weight_pct INT64 OPTIONS(description="Indicates the percentage the goal will contribute to the overall rating for all goals."),
  expected_result_text STRING OPTIONS(description="Indicates the result required for successful achievement of the goal"),
  goal_measurement_text STRING OPTIONS(description="Indicates how results for the goal will be measured"),
  goal_status_id INT64 OPTIONS(description="Indicates the status of the goal: Active or Inactive"),
  goal_progress_status_id INT64 OPTIONS(description="Indicates how the employee is progressing against the goal.  It includes values such as Not Started and Completed"),
  goal_performance_plan_id INT64 OPTIONS(description="unique sequence number is generated for performance management plan"),
  goal_due_date DATE OPTIONS(description="Indicates the date on which the goal must be achieved"),
  user_defined_date DATE OPTIONS(description="User defined date field.  This field can be used for any reason.  However, it it currently used by one division to store the goals due date.  "),
  review_year_num INT64 OPTIONS(description="The year that the goals apply towards."),
  review_period_end_date DATE OPTIONS(description="End date of the review period"),
  review_period_start_date DATE OPTIONS(description="Start date of the review period"),
  review_period_id INT64 OPTIONS(description="Period for which the review was executed"),
  manager_goal_performance_rating_id INT64 OPTIONS(description="Displays the manager rating as a numeric value"),
  manager_goal_performance_rating_num INT64 OPTIONS(description="The numeric rating by the manager of his or her performance against the goal."),
  employee_goal_performance_rating_id INT64 OPTIONS(description="Displays the employees rating of his or her performance against the goal"),
  employee_goal_performance_rating_num INT64 OPTIONS(description="The numeric rating by the employee of his or her performance against the goal."),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  valid_to_date DATETIME OPTIONS(description="End date of the record"),
  source_system_key STRING OPTIONS(description="A unique key coming from the source to track each record."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_goal_detail_sid
OPTIONS(description="This table maintains employees goal report information");