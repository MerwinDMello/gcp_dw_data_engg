CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_surgery_type
(
  surgery_type_id INT64 NOT NULL OPTIONS(description='A UNIQUE identifier FOR EACH surgery TYPE.'),
  surgery_type_desc STRING OPTIONS(description='The TYPE OF surgery that occured.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one CHARACTER code indicating the SPECIFIC SOURCE SYSTEM FROM which the DATA originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='TIMESTAMP OF UPDATE OR LOAD OF this record TO the Enterprise DATA Warehouse.')
)
CLUSTER BY surgery_type_id
OPTIONS(
  description='Contains a distinct list of surgery types.'
);
