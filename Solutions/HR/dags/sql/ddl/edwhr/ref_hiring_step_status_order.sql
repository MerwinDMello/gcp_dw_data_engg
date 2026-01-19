CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_hiring_step_status_order (
step_status_order_id INT64 NOT NULL OPTIONS(description="Unique identifier for each step and status combination.")
, step_name STRING OPTIONS(description="The name for each step a submission can take.")
, submission_status_name STRING OPTIONS(description="A name for each status of the submission process.")
, step_status_order_num INT64 OPTIONS(description="Reflects a suborder of step status combinations defined by the business.")
, step_status_name STRING OPTIONS(description="Name of the status and step defined by the business.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Step_Status_Order_Id
OPTIONS(description="Contains the order each step and status hiring combination goes through.");
