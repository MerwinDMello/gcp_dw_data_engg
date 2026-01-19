CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail
(
  recruitment_job_parameter_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each parameter of a job.")
, element_detail_entity_text STRING NOT NULL OPTIONS(description="The entity the detail is associated with.")
, element_detail_type_text STRING NOT NULL OPTIONS(description="The type of detail element.")
, element_detail_seq INT64 NOT NULL OPTIONS(description="The sequence number associated with the element.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, job_parameter_num INT64 OPTIONS(description="A unique number coming from the source for each parameter of a job.")
, element_detail_id INT64 OPTIONS(description="The element detail id that corresponds to the detail associated with the candidate.")
, element_detail_value_text STRING OPTIONS(description="The value of the element that corresponds to the detail associated with the candidate.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY recruitment_job_parameter_sid OPTIONS(description="Contains details around the job parameter based on user-defined fields in the source.");