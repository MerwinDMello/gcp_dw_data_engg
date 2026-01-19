CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_element_definition_type
(
  element_detail_definition_type_id INT64 NOT NULL OPTIONS(description="A unique identifier for the type of user defined field.")
, element_detail_definition_type_desc STRING OPTIONS(description="A description for the type of user defined field.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY element_detail_definition_type_id OPTIONS(description="Contains the different types of elements for user defined fields.");