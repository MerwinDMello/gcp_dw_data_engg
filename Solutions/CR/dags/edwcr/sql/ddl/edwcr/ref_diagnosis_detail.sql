CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail
(
  diagnosis_detail_id INT64 NOT NULL OPTIONS(description='A unique identifier for each diagnosis detail category.'),
  diagnosis_detail_desc STRING OPTIONS(description='A unique description for each diagnosis detail category'),
  diagnosis_indicator_text STRING OPTIONS(description='Text indicating where diagnosis is Metastatic , Rescetable or locally advanced'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY diagnosis_detail_id
OPTIONS(
  description='Contains the different queries associated the patient diagnosis.'
);
