CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_encounter
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='Account Number for the Encounter'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  patient_sk NUMERIC(29) OPTIONS(description='This element is the anchor surrogate key for the Patient data.'),
  facility_sk NUMERIC(29) OPTIONS(description='This element is the anchor surrogate key for the Provider data.'),
  medical_record_num STRING OPTIONS(description='The EHR patient medical record number.  '),
  patient_market_urn STRING OPTIONS(description='The EHR Medical Unique Record Number.  Meditech 5 and 6 specific.'),
  arrival_mode_sk INT64 OPTIONS(description='Identifies how the patient was brought to the healthcare facility.  (Source: ADM.Pat.arrived.from)'),
  arrival_mode_code STRING OPTIONS(description='This field contains the unique value for the arrival mode code'),
  arrival_mode_desc STRING OPTIONS(description='This field contains the description for arrival mode code'),
  admit_source_sk INT64 OPTIONS(description='This is also termed as Admit Source. It indicates the conditions or means by which the patient was admitted. For example: Physician Referral, Clinic Referral, Transfer from a Hospital, etc. MT:ADM.Pat.Admit.source'),
  admit_source_code STRING OPTIONS(description='This field has the unique value for admit source'),
  admit_source_desc STRING OPTIONS(description='This is the description of admit source'),
  admit_type_code STRING OPTIONS(description='This field indicates the circumstances under which the patient was admitted e.g., accident, routine, emergency etc'),
  visit_type_sk INT64 OPTIONS(description='Indicates the type of encounter (e.g. Emergency, Inpatient, Outpatient, etc)'),
  visit_type_code STRING OPTIONS(description='This field contains the unique value for the visit type code'),
  visit_type_desc STRING OPTIONS(description='This field contains the description for visit type code'),
  special_program_sk INT64 OPTIONS(description='This field designates the specific health insurance program for a visit required for healthcare reimbursement.'),
  special_program_code STRING OPTIONS(description='This field contains the unique value for the special programming code'),
  special_program_desc STRING OPTIONS(description='This field contains the description for the special programming code'),
  discharge_status_sk INT64 OPTIONS(description='This field contains the disposition of the patient at time of discharge (i.e., discharged to home, expired, etc.).r\rIn the US, this field is used on UB92 FL22. Refer to a UB specification for additional information.'),
  discharge_status_code STRING OPTIONS(description='This field contains the unique value for the discharge status code'),
  discharge_status_desc STRING OPTIONS(description='This field contains the description of the discharge status code'),
  encounter_date_time DATETIME OPTIONS(description='This can be the visit, admit, register, preadmit or arrival date time.'),
  admission_date_time DATETIME OPTIONS(description='This field contains the admit date/time. It is to be used if the event date/time is different than the admit date and time, i.e., a retroactive update. r\rThis field is also used to reflect the date/time of an out-patient/emergency patient registration.'),
  accident_date_time DATETIME OPTIONS(description='This field contains the date/time of the accident'),
  discharge_date_time DATETIME OPTIONS(description='This field contains the date/time of discharge.'),
  reason_for_visit_text STRING OPTIONS(description='Main patient\'s complaint at admission. (e.g. reason for visit)'),
  actual_los_cnt INT64 OPTIONS(description='This is number of days the patient actually stayed as in-patient also known as length of stay. Applies to In-Patient Only'),
  signature_date DATE OPTIONS(description='This field contains the date on which a signature was obtained for insurance billing purposes.'),
  readmission_ind STRING OPTIONS(description='This identifies if the patient is readmitted within 30 days of discharge.'),
  source_system_text STRING OPTIONS(description='This is information about the source system'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains all the patient encounters'
);
