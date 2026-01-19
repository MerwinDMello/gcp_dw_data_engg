CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_bi_movement_reason
(
  movement_reason_id INT64 NOT NULL OPTIONS(description="A unique number for each combination of employee actions."),
  external_candidate_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  lob_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  division_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  location_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  department_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  position_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  status_change_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes"),
  action_desc STRING OPTIONS(description="A summary of the change based on the compbination of employee actions."),
  reason_desc STRING OPTIONS(description="A summary reason of the change based on the combination of employee actions."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
OPTIONS(
  description="Contains derived movement reason based on certain inputs."
);