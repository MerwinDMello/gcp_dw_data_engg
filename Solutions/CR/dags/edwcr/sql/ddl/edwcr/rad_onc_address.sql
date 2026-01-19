CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_address
(
  address_sk INT64 NOT NULL OPTIONS(description='Unique surrogate Key generated for each address'),
  address_line_1_text STRING OPTIONS(description='The first line of the address of the patient or contact'),
  address_line_2_text STRING OPTIONS(description='The second line of the address of the patient or contact'),
  full_address_text STRING OPTIONS(description='Text for full address of patient or contact'),
  address_comment_text STRING OPTIONS(description='Comment for address'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY address_sk
OPTIONS(
  description='Contains the address of the patients and there contacts'
);
