create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_lawson_parameter`
(
  parameter_code_1 STRING NOT NULL OPTIONS(description="code from lawson that can denote a variety of categories like shift type or compensation market."),
  parameter_code_2 STRING NOT NULL OPTIONS(description="code from lawson that can denote a variety of categories like shift type or compensation market."),
  parameter_code_3 STRING NOT NULL OPTIONS(description="code from lawson that can denote a variety of categories like shift type or compensation market."),
  parameter_group_code STRING NOT NULL OPTIONS(description="code that uniquely identifies the category."),
  detail_value_text STRING OPTIONS(description="description of the value belonging to the key."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY parameter_code_1, parameter_code_2, parameter_code_3, parameter_group_code
OPTIONS(
  description="this table contains various lawson codes and their values."
);