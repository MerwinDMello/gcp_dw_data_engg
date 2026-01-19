CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_junc_physician_phone
(
  physician_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each physician'),
  phone_num_type_code STRING NOT NULL OPTIONS(description='Identifies the type of phone number - home, cell, work, pager,transportation'),
  phone_num_sk INT64 OPTIONS(description='The phone number of the patient'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY physician_sk, phone_num_type_code
OPTIONS(
  description='This table will be a junction for physician phone numbers'
);
