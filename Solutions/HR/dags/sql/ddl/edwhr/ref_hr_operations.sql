/*edwhr.ref_hr_operations*/


CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_hr_operations (
process_level_code STRING NOT NULL OPTIONS(description="This field maintains Process level code of a facility")
, business_unit_name STRING NOT NULL OPTIONS(description="It captures the name of business unit relevant to HR operations")
, business_unit_segment_name STRING NOT NULL OPTIONS(description="It contains the unique segment name for a given business unit and process level code")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY process_level_code
OPTIONS(description="This table captures the attributes of Human Resource Operations, with each process level having an assigned business unit name and segment name.");
