CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_care_team
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  role_plyr_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the surrogate key of the role (e.g. Practitioner or patient)'),
  practitioner_role_type STRING NOT NULL OPTIONS(description='Code to identify the practitioner role'),
  practitioner_role_type_desc STRING OPTIONS(description='Description of the practitioner role type'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  practitioner_first_name STRING OPTIONS(description='It indicates the given name of Party.'),
  practitioner_middle_name STRING OPTIONS(description='It indicates the middle initial or 1 or more middle names of Party.'),
  practitioner_last_name STRING OPTIONS(description='It indicates the party name component representing the family or last name of the party.'),
  practitioner_full_name STRING OPTIONS(description='Complete name for a Party as one string.'),
  practitioner_specialty_name STRING OPTIONS(description='Describes the specialty of the provider'),
  practitioner_mnemonic_cs STRING OPTIONS(description='Primary alpha-numeric identifier for a Role Player\'s registration'),
  practitioner_npi NUMERIC(29) OPTIONS(description='Primary alpha-numeric identifier for a Role Player\'s registration'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains information about the providers that were involved in a patient\'s encounter.'
);
