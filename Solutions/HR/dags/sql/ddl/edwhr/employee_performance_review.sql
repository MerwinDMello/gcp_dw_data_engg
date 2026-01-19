create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_performance_review`
(
  employee_sid INT64 NOT NULL OPTIONS(description="unique numeric number generated for unique combination of employee identifier and hr company identifier"),
  review_sequence_num INT64 NOT NULL OPTIONS(description="this contains system generated review sequence number as review records are created"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  reviewer_employee_sid INT64 OPTIONS(description="unique numeric number generated for unique combination of reviewer employee identifier and hr company identifier"),
  scheduled_review_date DATE OPTIONS(description="this field contains the scheduled review date."),
  review_type_code STRING OPTIONS(description="this field contains the review type code."),
  actual_review_date DATE OPTIONS(description="this field contains the actual review date."),
  performance_rating_code STRING OPTIONS(description="this field contains the performance rating"),
  last_update_date DATE OPTIONS(description="this field contains the server date (month, date, and year) of the last change to this record."),
  last_update_time TIME OPTIONS(description="contains the server time (hour, minute,and second) when the record was last changed."),
  last_updated_3_4_login_code STRING OPTIONS(description="contains the user 3 4 id of the person who last changed this record."),
  total_score_num NUMERIC OPTIONS(description="this field contains employees total performance score."),
  review_desc STRING OPTIONS(description="review description of employee"),
  review_schedule_type_code STRING OPTIONS(description="this filed contains employee review schedule type."),
  next_review_date DATE OPTIONS(description="it contains employees next review date."),
  next_review_code STRING OPTIONS(description="it contains employees next review code."),
  last_review_date DATE OPTIONS(description="it contains employees last review date"),
  employee_num INT64 NOT NULL OPTIONS(description="employee number of an employee associated with hr lawson company"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="unique code of an hr company facility or process level. this field is spaces for hr company records"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_sid, review_sequence_num
OPTIONS(
  description="this table contains information pertaining to employee performance reviews for hos employees and anyone not in tms. "
);