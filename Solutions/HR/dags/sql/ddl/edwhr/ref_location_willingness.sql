CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_location_willingness
(
  location_willingness_id INT64 NOT NULL OPTIONS(description="A unique identifier for each willing to travel or relocate value."),
  location_willingness_desc STRING OPTIONS(description="The discrete willing to travel or relocate values."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY location_willingness_id
OPTIONS(description="Contains the discrete values of willing to travel and relocate responses.");