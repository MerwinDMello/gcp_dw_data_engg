CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_biopsy
(
  procedure_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the anchor surrogate key for the Procedure data.'),
  procedure_text STRING OPTIONS(description='This field contains a value that uniquely identifies a single procedure for an encounter. It is unique across all segments and messages for an encounter.'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  biopsy_ts DATETIME OPTIONS(description='This field contains the date/time that the procedure was performed.'),
  biopsy_performing_physician_name STRING OPTIONS(description='Complete name for a Party as one string.'),
  physician_specialty_name STRING OPTIONS(description='Describes the specialty of the provider'),
  role_plyr_sk NUMERIC(29) OPTIONS(description='The ID assigned to uniquely identify all staff associated with provider, research, clinical care and associated users of the system.'),
  physician_npi NUMERIC(29) OPTIONS(description='Primary alpha-numeric identifier for a Role Player\'s registration'),
  priority_sequence_num INT64 NOT NULL OPTIONS(description='This field contains the number that identifies the significance or priority of the Procedure code.'),
  anesthesia_code_sk INT64 OPTIONS(description='This field contains a unique identifier of the anesthesia used during the procedure.'),
  anesthesia_code_desc STRING OPTIONS(description='A textual description of the system of letters or numbers used for identification purposes'),
  encounter_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the anchor surrogate key for the Encounter data.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains all the patient biopsies. The data comes from the table PRCDR_DTL and the field PRCDR_TXT has been filtered to only capture those records that contain the word Biopsy.'
);
