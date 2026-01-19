create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_pay_grade`
(
  pay_grade_code STRING NOT NULL OPTIONS(description="unique list of pay grade codes are maintained in this field"),
  pay_grade_desc STRING OPTIONS(description="description of pay grade code maintained in this field"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY pay_grade_code
OPTIONS(
  description="this table maintains unique list of pay grade codes. pay grade codes are used to identify pay of an individual employee. "
);