create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_comment_type`
(
  comment_type_code STRING NOT NULL OPTIONS(description="contains 2 letter code used to designate each type of comment."),
  comment_type_desc STRING OPTIONS(description="description of the comment type code."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY comment_type_code
OPTIONS(
  description="contains comment codes and descriptions for each type of comment used for employees or applicants."
);