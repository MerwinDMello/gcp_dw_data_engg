CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_address
(
  nav_patient_id NUMERIC(29) NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  housing_type_id INT64 OPTIONS(description='Identifier for type of housing'),
  address_line_1_text STRING OPTIONS(description='The first line of the address of the patient.'),
  address_line_2_text STRING OPTIONS(description='The second line of the address of the patient.'),
  city_name STRING OPTIONS(description='The name of the city corresponding to the patients address.'),
  state_code STRING OPTIONS(description='The two character state code on the address.'),
  zip_code STRING OPTIONS(description='The zip code of a patient.'),
  local_housing_address_text STRING OPTIONS(description='Patient local housing address'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id
OPTIONS(
  description='Contains the address of the patients navigated by Sarah Cannon'
);
