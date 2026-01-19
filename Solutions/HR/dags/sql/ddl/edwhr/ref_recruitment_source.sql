CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_recruitment_source
(
  recruitment_source_id INT64 NOT NULL OPTIONS(description="A unique identifier of the recruiting source. Ex. Internet, job board, etc."),
  recruitment_source_desc STRING OPTIONS(description="A description of the recruiting source. Ex. internet, job board, etc."),
  recruitment_source_type_id INT64 OPTIONS(description="A unique identifier for each recruiting source type."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY recruitment_source_id
OPTIONS(
  description="Table contains a list of different recruitment sources used in the recruitment process."
);