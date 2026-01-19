CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_motive
(
motive_id INT64 NOT NULL OPTIONS(description="A unique identifier for each motive an event can have in the submission process.")
, active_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, motive_name STRING OPTIONS(description="A name for each motive an event can have in the submission process.")
, motive_code STRING OPTIONS(description="A code used for a short description for each motive in  the submission process.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY motive_id
OPTIONS(description="Contains a list of different motives that a submission event can have.");