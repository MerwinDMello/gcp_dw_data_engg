CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_patient
(
  role_plyr_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the anchor surrogate key for the Patient data'),
  empi_text STRING NOT NULL OPTIONS(description='It indicates the universal patient identifier set in MDM.'),
  patient_race_code_sk STRING OPTIONS(description='This is the surrogate key for race code'),
  patient_race_desc STRING OPTIONS(description='This field contains the description for race code'),
  address_line_1_text STRING OPTIONS(description='It indicates the first line of the address.  Used for Primary Street and Building Number.'),
  address_line_2_text STRING OPTIONS(description='It indicates the second line of the address.'),
  city_name STRING OPTIONS(description='It indicates the city of the address.'),
  state_code STRING OPTIONS(description='Standard code values for states or provinces.'),
  zip_code STRING OPTIONS(description='Zip code and Internationl Postal Code for the address'),
  home_phone_num STRING OPTIONS(description='Represents the home telephone number for the telephone, cell, or pager number'),
  business_phone_num STRING OPTIONS(description='Represents the business or work telephone number for the telephone, cell, or pager number'),
  mobile_phone_num STRING OPTIONS(description='Represents the mobile telephone number for the telephone, cell, or pager number'),
  birth_date_time DATETIME OPTIONS(description='Date and time patient was born.'),
  death_date_time DATETIME OPTIONS(description='Date and time patient was born.'),
  first_name STRING OPTIONS(description='It indicates the given name of Party.'),
  middle_name STRING OPTIONS(description='It indicates the middle initial or 1 or more middle names of Party.'),
  last_name STRING OPTIONS(description='It indicates the party name component representing the family or last name of the party.'),
  gender_code STRING OPTIONS(description='Unique coded value for gender.'),
  patient_email_text STRING OPTIONS(description='E-mail address string, representing the contact home e-mail address'),
  vital_status_id STRING OPTIONS(description='Flag that identifies if the person is still alive.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY role_plyr_sk
OPTIONS(
  description='Contains patient information'
);
