CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_position_pfte (
position_code_desc STRING NOT NULL OPTIONS(description="This is the position description.")
, pfte_value_num NUMERIC OPTIONS(description="This field maintains the percent of what a recruiters capacity is for requisition load management. This will help to get a better idea of capacity, since not all the FTEs in the department carry requisition loads")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Position_Code_Desc
OPTIONS(description="This table maintains unique list of Position Codes. This will help to identify by Position Code descriptions what a recruiters capacity is for requisition load management. This table will be load trough Ataccama RDM.");
