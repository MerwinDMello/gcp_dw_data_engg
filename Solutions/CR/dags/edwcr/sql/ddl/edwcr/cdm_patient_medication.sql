CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_patient_medication
(
  medication_admn_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the anchor surrogate key for the Medicication Administration event'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL,
  medication_desc STRING OPTIONS(description='This is the description for the medication code'),
  occurence_ts DATETIME OPTIONS(description='This is the date and time when the event actually occurred. See Planned Ts for the date and time when the event was planned to occur.'),
  drug_dose_amt_text STRING OPTIONS(description='This is the total dosage amount that was actually administered.'),
  drug_dose_measurement_text STRING OPTIONS(description='This is the unit of measure for the dose for this eMAR event.'),
  administrative_frequency_text STRING OPTIONS(description='This is field indicates the frequency this medication should be given.'),
  administered_unit_cnt INT64 OPTIONS(description='This is the unit amount that was administered for this eMAR event. (1 or 2 pills)'),
  route_code_sk STRING OPTIONS(description='It identifies the route of administration.'),
  route_code_desc STRING OPTIONS(description='A textual description of the system of letters or numbers used for identification purposes'),
  ordering_physician_name STRING OPTIONS(description='Complete name for a Party as one string.'),
  physician_npi NUMERIC(29) OPTIONS(description='Primary alpha-numeric identifier for a Role Player\'s registration'),
  medication_num_text STRING OPTIONS(description='This element is the anchor surrogate key for the Order data'),
  source_system_original_code STRING OPTIONS(description='Code from the souce system.'),
  clinical_pharmacy_trade_name STRING OPTIONS(description='The group type code designates the drug grouping classification such as therapeutic, general, specific, etc.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains the information of the medication administered to the patient'
);
