CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_probability_potential
(
  probability_potential_id INT64 NOT NULL OPTIONS(description="This is the unique sequence number generated for each potential description mainatained in performance management system"),
  probability_potential_desc STRING NOT NULL OPTIONS(description="This field maintain description of potential mainatinad in performance management system"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY probability_potential_id
OPTIONS(description="This table maintain unique list of probability potential information");