CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_position
(
  recruitment_position_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each position.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME NOT NULL OPTIONS(description="Date on which the record was invalidated.")
, recruitment_position_num INT64 NOT NULL OPTIONS(description="The position number unique to each position.")
, gsd_pct NUMERIC OPTIONS(description="An additional amount an employee can qualify for based on cost of living differences.")
, incentive_payout_pct NUMERIC OPTIONS(description="Percentage of payout if a position is eligible for an incentive plan.")
, incentive_plan_name STRING OPTIONS(description="The name of the incentive plan available for a position.")
, incentive_plan_potential_pct NUMERIC OPTIONS(description="Percent of salary a position can earn up to.")
, special_program_name STRING OPTIONS(description="Contains the name of any specialty program associated with the position (StaRN, Surgery, etc.)")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
PARTITION BY DATE(valid_from_date)
CLUSTER BY recruitment_position_sid OPTIONS(description="Contains information about the position from the recruitment system.");