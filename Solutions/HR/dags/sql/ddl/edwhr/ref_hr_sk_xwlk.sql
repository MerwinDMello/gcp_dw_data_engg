create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk`
(
  hr_sk NUMERIC NOT NULL OPTIONS(description="this is the sk that was generated based on the natural key."),
  hr_sk_type_text STRING NOT NULL OPTIONS(description="this is the type of sk that was generated for the subject of the sk."),
  hr_sk_source_text STRING OPTIONS(description="this is the natural key from the source concatenated together."),
  hr_sk_generated_date_time DATETIME OPTIONS(description="this is the date time the sk was generated."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY hr_sk, hr_sk_type_text
OPTIONS(
  description="contains all surrogate key values that is generated in etl for the lawson tables."
);