CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_workforce_planning_target (
division_name STRING NOT NULL OPTIONS(description="This is the name assigned in DMR/Company Master for the Division.")
, measure_name STRING NOT NULL OPTIONS(description="Contains the measure name. Ex. External Hires, Starting Headcount, etc.")
, period_end_date DATE NOT NULL OPTIONS(description="Date on which the measure ended.")
, metric_cnt INT64 OPTIONS(description="Contains the count of the measure.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Division_Name, Measure_Name, Period_End_Date
OPTIONS(description="This table contains the Workforce planning target by divisions.");
