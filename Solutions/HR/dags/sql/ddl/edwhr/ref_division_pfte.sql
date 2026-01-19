CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_division_pfte (
division_name STRING NOT NULL OPTIONS(description="This is the division name as entered in the source.")
, period_end_date DATE NOT NULL OPTIONS(description="Date on which the measure ended.")
, division_abbreviation_code STRING OPTIONS(description="This is the division abbreviation code.")
, pfte_value_num NUMERIC OPTIONS(description="Contains the amount of full time employees for each division.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Division_Name, Period_End_Date
OPTIONS(description="This table contains the data needed to measure the Division wide FTE productivity. This table will be loaded through Ataccama RDM.");
