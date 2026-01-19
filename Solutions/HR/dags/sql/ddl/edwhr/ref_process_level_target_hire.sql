CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_process_level_target_hire (
process_level_sid INT64 NOT NULL OPTIONS(description="ETL generated unique value for an Process level code")
, period_year_month_code STRING NOT NULL OPTIONS(description="This is the Year and Month.")
, process_level_code STRING NOT NULL OPTIONS(description="This field maintains Process level code of an facility")
, target_hire_num BIGNUMERIC OPTIONS(description="The number of hirers targeted for each month.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Process_Level_SID, Period_Year_Month_Code
OPTIONS(description="Contains the number of hires each process level is targeting to hire per month.");
