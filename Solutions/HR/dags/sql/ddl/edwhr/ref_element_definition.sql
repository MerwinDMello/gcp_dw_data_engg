CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_element_definition
(
  element_detail_entity_text STRING NOT NULL OPTIONS(description="The entity the detail is associated with.")
, element_detail_type_text STRING NOT NULL OPTIONS(description="The type of detail element.")
, element_detail_definition_desc STRING OPTIONS(description="The description of the element for the user defined field.")
, element_detail_definition_type_id INT64 OPTIONS(description="A unique identifier for the type of user defined field.")
, element_definition_selection_id INT64 OPTIONS(description="A unique identifier for the selection of each element.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY element_detail_entity_text, element_detail_type_text OPTIONS(description="Contains the definition of each User Defined Field (UDF) element.");