CREATE TABLE IF NOT EXISTS {{ params.param_fs_core_dataset_name }}.ref_fa_measure (
fa_measure_code STRING NOT NULL OPTIONS(description="This is the code assigned to the measure such as ADM_FC_COM_MGD_ALL_X14, I1840.")
, fa_measure_desc STRING OPTIONS(description="This is the description of the measure.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY FA_Measure_Code
OPTIONS(description="This table contains the descriptions for the Financial Analytics Measure used in the reverse interface table loaded from the Financial Analytics Essbase Cube.");
