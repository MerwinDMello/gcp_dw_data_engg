CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_process_level_union
(
process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value mainatined in this field.")
, union_code STRING NOT NULL OPTIONS(description="The 5 character union code that represents the employee union.")
, union_name STRING OPTIONS(description="Description of the employee union.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY process_level_code, union_code
OPTIONS(description="Contains a list of process levels and the employee unions that work at the process level.");
