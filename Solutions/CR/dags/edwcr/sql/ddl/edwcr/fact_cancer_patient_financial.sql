CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_cancer_patient_financial
(
  encounter_id NUMERIC(29) NOT NULL OPTIONS(description='These are the event level IDs for each of the databases included in this table. Service_ID, Patient_DW_ID, and Encounter_DW_ID for EDWCP.Service, EDWPF.Fact_Patient, and EDWPS.Encounter respectively. '),
  encounter_source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  empi_text STRING OPTIONS(description='Identifies a patient across care settings.'),
  pat_acct_num STRING OPTIONS(description='Patient account number coming from the source system.'),
  medical_record_num STRING OPTIONS(description='The patient\'s medical record number from a hospital setting.'),
  patient_market_urn STRING OPTIONS(description='The patient\'s market urn that can track a patient for individual meditech markets. Null for other care settings.'),
  encounter_start_date DATE OPTIONS(description='Date the encounter began.'),
  encounter_end_date DATE OPTIONS(description='Date the encounter ended.'),
  coid STRING OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING OPTIONS(description='The company identifier which uniquely identifies the data associated company.'),
  sub_unit_num STRING OPTIONS(description='This is a number assigned to a sub processing unit of a Patient Accounting processing unit.'),
  diagnosis_type_code STRING OPTIONS(description='This determines if the Diagnosis Code is ICD9 or ICD10.\t'),
  principal_diagnosis_code STRING OPTIONS(description='Final or principal diagnosis code.'),
  procedure_type_code STRING OPTIONS(description='CPT, HCPSC, 9, 10, or 5.'),
  principal_procedure_code STRING OPTIONS(description='Final or principal procedure code.'),
  principal_procedure_date DATE OPTIONS(description='Date of the principal procedure.'),
  total_billed_charge_amt NUMERIC(32, 3) OPTIONS(description='Total billed charges for an encounter.'),
  patient_age INT64 OPTIONS(description='Age of the patient'),
  patient_person_dw_id NUMERIC(29) OPTIONS(description='Person DW ID for the given database source.'),
  financial_class_code INT64 OPTIONS(description='Financial class code of the patient.'),
  patient_type_code_pos1 STRING OPTIONS(description='The first character of the patient type code.'),
  emergency_ind STRING OPTIONS(description='Indicates if the encounter was classified as an emergency. Derived from revenue codes 450-459 and total factor quantity > 0 or admitting patient type is emergency or previous admitting patient type is emergency.'),
  oncology_tumor_site_id INT64 OPTIONS(description='Site of the tumor if patient has an oncology related occurence.'),
  oncology_detail_tumor_site_id INT64 OPTIONS(description='Detailed site of the tumor if patient has an oncology related occurence.'),
  robotic_ind3 STRING OPTIONS(description='The indicator is usually a \'Y\'/ \'N\' (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation. Based off of Robotic_Ind and Robotic_Ind2.\r'),
  op_product_line_desc STRING OPTIONS(description='Outpatient Product Line description based on Revenue and Procedure codes.'),
  drg_code_hcfa STRING OPTIONS(description='Code which identifies groups of patient encounters or services for a prospective reimbursement methodology.  These groups generally consist of encounters or services that are similar in severity and/or cost of treatment for the purpose for assigning the D'),
  drg_medical_surgical_ind STRING OPTIONS(description='Indicates whether the DRG includes patients with operating room procedures'),
  calculated_length_of_stay_num NUMERIC(33, 4) OPTIONS(description='The arithmetic mean expected length of stay for patients in the DRG'),
  estimated_net_revenue_amt NUMERIC(32, 3) OPTIONS(description='Amount of estimated net revenue'),
  direct_contribution_margin_amt NUMERIC(32, 3) OPTIONS(description='The direct contribution margian for the given encounter.'),
  ebdita_amt NUMERIC(32, 3) OPTIONS(description='Amount of earning before depreciation interest taxes ammortization'),
  esl_level_1_desc STRING OPTIONS(description='Description for Enterprise Service Line level 1. This is dependent on the Patient and not exclusively on the ESL Code'),
  esl_level_2_desc STRING OPTIONS(description='Description for Enterprise Service Line level 2. This is dependent on the Patient and not exclusively on the ESL Code'),
  esl_level_3_desc STRING OPTIONS(description='Description for Enterprise Service Line level 3. This is dependent on the Patient and not exclusively on the ESL Code'),
  esl_level_4_desc STRING OPTIONS(description='Description for Enterprise Service Line level 4. This is dependent on the Patient and not exclusively on the ESL Code'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY encounter_id, encounter_source_system_code
OPTIONS(
  description='Contains fact data related to patient financials for Hospital encounters.This will be used to pull financial data in reporting layer'
);
