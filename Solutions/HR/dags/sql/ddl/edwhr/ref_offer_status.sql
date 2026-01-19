
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_offer_status
 (
offer_status_id INT64 NOT NULL OPTIONS(description="A unique identifier fro each status an offer can have.")
, offer_status_desc STRING OPTIONS(description="The status of the offer.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY offer_status_id
OPTIONS(description="Contains a unique list of offer statuses.");