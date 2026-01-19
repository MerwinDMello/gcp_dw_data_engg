CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type
(
  recruitment_source_type_id INT64 NOT NULL OPTIONS(description="A unique identifier for each recruiting source type."),
  recruitment_source_type_code STRING OPTIONS(description="A code that identifies the type of recruiting source."),
  recruitment_source_type_desc STRING OPTIONS(description="A description of the recruiting source type."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY recruitment_source_type_id
OPTIONS(
  description="Table contains a list of different recruitment source types used in the recruiting process."
);