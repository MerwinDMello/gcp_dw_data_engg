CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_contact
(
  cn_patient_contact_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  contact_purpose_id INT64 OPTIONS(description='A unique identifier for each contact purpose.'),
  contact_method_id INT64 OPTIONS(description='A unique identifier for each contact method.'),
  contact_person_id INT64 OPTIONS(description='A unique identifier for each distinct person contacted.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  contact_date DATE OPTIONS(description='Date the contact was made.'),
  other_purpose_detail_text STRING OPTIONS(description='Detailed purpose of contact.'),
  other_person_contacted_text STRING OPTIONS(description='Detailed person contacted.'),
  time_spent_amount_text STRING OPTIONS(description='The amount of time spent contacting the person.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains all the communications to the patient or other points of contacts associated with the patient.'
);
