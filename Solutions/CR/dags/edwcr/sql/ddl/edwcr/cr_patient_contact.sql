CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_contact
(
  patient_contact_id INT64 NOT NULL OPTIONS(description='Unique idenfier for patient contact'),
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  contact_relation_id INT64 OPTIONS(description='Identifier for relation of the contact person with patient'),
  contact_type_id INT64 OPTIONS(description='Identifier for Type of Contact'),
  contact_num_code STRING OPTIONS(description='Code for identifying the contact person'),
  contact_first_name STRING OPTIONS(description='The first name of the patient.'),
  contact_last_name STRING OPTIONS(description='The persons last name.'),
  contact_middle_name STRING OPTIONS(description='The middle name of the person.'),
  preferred_contact_method_text STRING OPTIONS(description='Text for preferred contact method'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_contact_id, cr_patient_id
OPTIONS(
  description='Contains the patient contact person details'
);
