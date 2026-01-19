CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_personnel_pay_type (
personnel_pay_type_code STRING NOT NULL OPTIONS(description="This code is specific to hourly employees and classifies them based off of their pay type.")
, personnel_pay_type_desc STRING OPTIONS(description="Describes the pay type for the hourly employee.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Personnel_Pay_Type_Code
OPTIONS(description="This tables contains the pay types that hourly employees can fall under.");
