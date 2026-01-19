create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_lawson_element`
(
  lawson_element_num INT64 NOT NULL OPTIONS(description="contains the lawson hr data  dictionary unique field number"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  lawson_topic_code STRING OPTIONS(description="topic is the area or subject in lawson system for which lawson element number is associated"),
  lawson_element_desc STRING OPTIONS(description="contains the lawson data item (field)  description"),
  lawson_element_type_flag STRING OPTIONS(description="indicates the data type.a = alphanumeric,  d = date ,  n = numeric"),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY lawson_element_num
OPTIONS(
  description="contains a record for each lawson database element you can use"
);