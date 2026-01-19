CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_patient_diagnosis
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='A system generated number used to uniquely identify a patient.'),
  diagnosis_cycle_code STRING NOT NULL OPTIONS(description='Identifies the diagnosis code as A = Admitting, I = Interim, F = Final'),
  diagnosis_mapped_code STRING NOT NULL OPTIONS(description='Field that indicates by a Y or N if a daignosis code has been mapped.'),
  diagnosis_code STRING NOT NULL OPTIONS(description='The diagnosis code which indicates that a patient\'s condition is complicated.  This is a standard ICD-9-CM code used to identify the patients diagnosis at discharge.  Up to 15 occurrences of diagnosis codes are maintained with Diag_Rank_Num.  (on mainfra'),
  diagnosis_type_code STRING NOT NULL OPTIONS(description='Code to identify a diagnosis as an ICD9 or other types'),
  diagnosis_type_code_desc STRING NOT NULL OPTIONS(description='The diagnosis type code description. ICD-09 when the diagnosis type code is 9 and ICD-10 when the diagnosis type code is 0.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  diagnosis_rank_num INT64 OPTIONS(description='The precedence of the diagnosis in relation to other diagnoses of the patient'),
  diagnosis_short_desc STRING OPTIONS(description='Brief English description for a diagnosis code'),
  cancer_diagnosis_ind STRING OPTIONS(description='Indicates if the patient has cancer or not. '),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains all diagnoses at the encounter level.'
);
