CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_hiring_step_order (
step_order_id INT64 NOT NULL OPTIONS(description="Each number corresponds to a different step in the hiring process.")
, step_name STRING OPTIONS(description="The name for each step a submission can take.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Step_Order_Id
OPTIONS(description="Contains the order of the steps used in the hiring process.");
