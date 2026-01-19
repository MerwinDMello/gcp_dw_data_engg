CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_detail (
candidate_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each candidate.")
, element_detail_entity_text STRING NOT NULL OPTIONS(description="The entity the detail is associated with.")
, element_detail_type_text STRING NOT NULL OPTIONS(description="The type of detail element.")
, element_detail_seq INT64 NOT NULL OPTIONS(description="The sequence number associated with the element.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, element_detail_id NUMERIC OPTIONS(description="The element detail id that corresponds to the detail associated with the candidate.")
, element_detail_value_text STRING OPTIONS(description="The value of the element that corresponds to the detail associated with the candidate.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Candidate_SID
OPTIONS(description="Contains details around the candidate based on user-defined fields in the source.");
