CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_competency
(
  competency_id INT64 NOT NULL OPTIONS(description="This field maintains unique sequence number for each competency available in employee performance management system"),
  competency_desc STRING NOT NULL OPTIONS(description="This field maintains description of competency"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY competency_id
OPTIONS(description=("This table maintains list of all competency available in performance management system"));
