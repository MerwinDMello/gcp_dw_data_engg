CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_phone
(
  phone_num_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each phone number'),
  phone_num_text STRING OPTIONS(description='The phone number of the patient or contact person'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY phone_num_sk
OPTIONS(
  description='Contains the different phone numbers a patient , contact or doctor could be associated with.'
);
