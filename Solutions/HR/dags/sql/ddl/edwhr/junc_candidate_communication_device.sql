CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.junc_candidate_communication_device
(
  communication_device_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each communication device number."),
  candidate_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each candidate."),
  communication_device_type_code STRING NOT NULL OPTIONS(description="Code that identifies if the communication device number is personal, work, home, cell."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded."),
  valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_sid
OPTIONS(
  description="Connects the candidate to their different communication devices."
);
