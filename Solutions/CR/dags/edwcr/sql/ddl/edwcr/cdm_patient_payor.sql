CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_patient_payor
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='A system generated number used to uniquely identify a patient.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  payor_dw_id_ins1 NUMERIC(29) OPTIONS(description='Data Warehouse assigned ID used to distinguish the primary PAYOR. (Primary Insurance Payor Code)'),
  payor_name STRING OPTIONS(description='Name of the Payer for the associated insurance plan.  Data for the field is obtained from the insurance plan master file.'),
  iplan_id_ins1 INT64 OPTIONS(description='An identification code assigned to the primary insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  plan_desc STRING OPTIONS(description='Description associated with an insurance plan.'),
  major_payor_id_ins1 NUMERIC(29) OPTIONS(description='Unique Identifier assigned by the Data Warehouse.'),
  major_payor_group_desc STRING OPTIONS(description='The business name for the major payor group, such as Aetna, Cigna, United Healthcare, etc.'),
  financial_class_code NUMERIC(29) OPTIONS(description='The Financial Class (formerly known as revenue category) is financial class assigned to the patients account based on the financial class associated with his primary insurance plan in the hospitals insurance plan master file.'),
  financial_class_desc STRING OPTIONS(description='Text description of the financial class assigned to the patients account based on the financial class associated with his primary insurance plan in the hospitals insurance plan master file.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Payor information at the Patient level. Contains the primary insurance of the patient.'
);
