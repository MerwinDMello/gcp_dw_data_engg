CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_element_detail
(
  element_detail_id INT64 NOT NULL OPTIONS(description="The element detail id that corresponds to the detail associated with the candidate.")
, element_definition_selection_id INT64 NOT NULL OPTIONS(description="A unique identifier for the selection of each element.")
, active_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, element_code STRING OPTIONS(description="A code for the element response value.")
, element_desc STRING OPTIONS(description="A description of the element response value.")
, element_seq_num INT64 OPTIONS(description="The sequence of the element response value.")
, complete_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, eff_from_date_time DATETIME OPTIONS(description="Date that the response value is effective from for a given element")
, eff_to_date_time DATETIME OPTIONS(description="Date that the response value is effective to for a given element")
, last_modified_date_time DATETIME OPTIONS(description="Date that the element was last modified.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY element_detail_id, element_definition_selection_id OPTIONS(description="A standard response for each user defined field based on the selection type.");