CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status
(
  recruitment_requisition_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each requisition coming from the recruitment system.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, requisition_status_id INT64 OPTIONS(description="The status of the requisition.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY recruitment_requisition_sid OPTIONS(description="Contains details around the requisition status from the recruitment system.");