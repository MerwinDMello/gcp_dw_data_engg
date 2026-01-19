CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_element_definition_selection
(
  element_definition_selection_id INT64 NOT NULL OPTIONS(description="A unique identifier for the selection of each element.")
, element_definition_selection_code STRING OPTIONS(description="A code for the selection definition.")
, element_definition_selection_name STRING OPTIONS(description="A description for the selection definition.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY element_definition_selection_id OPTIONS(description="Contains the defined selection for each element of user defined element.");