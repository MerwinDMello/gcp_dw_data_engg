CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_requisition_status
(
requisition_status_id INT64 NOT NULL OPTIONS(description="A unique number for each requisition status.")
, status_desc STRING OPTIONS(description="The description of the status.")
, parent_requisition_status_id INT64 OPTIONS(description="The parent status id for the current status. Used to group statuses together.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY requisition_status_id
OPTIONS(description="Contains the different statuses of a requisition");