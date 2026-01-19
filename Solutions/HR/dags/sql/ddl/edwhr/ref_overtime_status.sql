CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_overtime_status
(
overtime_status_id INT64 NOT NULL OPTIONS(description="A unique identifier for each overtime status.")
, overtime_status_desc STRING OPTIONS(description="The description of different overtime statuses.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY overtime_status_id
OPTIONS(description="Contains a distinct list of overtime statuses of a job.");