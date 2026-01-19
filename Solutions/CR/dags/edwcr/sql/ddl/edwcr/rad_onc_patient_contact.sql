CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact
(
  patient_contact_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for patient contact'),
  patient_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  contact_address_sk INT64 OPTIONS(description='Identifier for patient conatct address'),
  contact_full_name STRING OPTIONS(description='Full name of the contact for patient'),
  contact_relation_text STRING OPTIONS(description='Text for relation of contact with patient'),
  contact_entrusted_ind STRING OPTIONS(description='Indicator if contact is entrusted or not'),
  contact_comment_text STRING OPTIONS(description='Text for contact comments'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_contact_sk
OPTIONS(
  description='Contains the patient contact person details'
);
