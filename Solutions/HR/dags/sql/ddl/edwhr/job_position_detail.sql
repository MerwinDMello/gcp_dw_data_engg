create table if not exists `{{ params.param_hr_core_dataset_name }}.job_position_detail`
(
  position_sid INT64 NOT NULL OPTIONS(description="unique etl generated sequence number for each user defined codes that represents a position in the hr company."),
  position_type_id STRING NOT NULL OPTIONS(description="contains a code that describes the class of data the record is about (1 = position)"),
  position_detail_code STRING NOT NULL OPTIONS(description="this number describes the category of the position data (89 = wfh position)"),
  eff_from_date DATE NOT NULL OPTIONS(description="represents the date the employee position began."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="effective start date of the record or load date."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  eff_to_date DATE NOT NULL OPTIONS(description="represents the end date of the position."),
  detail_value_alphanumeric_text STRING OPTIONS(description="contains the user defined alphanumeric value."),
  detail_value_num INT64 OPTIONS(description="contains the user defined numeric value."),
  detail_value_date DATE OPTIONS(description="contains the user defined date value."),
  lawson_object_id INT64 NOT NULL OPTIONS(description="contains the unique identifier that ties the transaction back to lawson."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value maintained in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_to_date)
CLUSTER BY position_sid, position_type_id, position_detail_code
OPTIONS(
  description="this table contains user defined fields related to the position."
);