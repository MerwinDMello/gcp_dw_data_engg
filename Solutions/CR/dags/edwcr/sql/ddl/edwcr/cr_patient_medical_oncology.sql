CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_medical_oncology
(
  drug_id INT64 NOT NULL OPTIONS(description='A unique identifier for drug'),
  treatment_id INT64 OPTIONS(description='Unique identifier for a treatment'),
  cycle_id INT64 OPTIONS(description='A unique identifier for cycle'),
  drug_route_id INT64 OPTIONS(description='Unique identifer for drug route'),
  drug_dose_unit_id INT64 OPTIONS(description='Identifier for units of dose given'),
  drug_hospital_id INT64 OPTIONS(description='Facility where the drug was given'),
  nsc_id INT64 OPTIONS(description='Unique Identifier for National Service Center Codes and subcodes'),
  total_drug_dose_amt NUMERIC(31, 2) OPTIONS(description='Total dosage amount for the drug'),
  drug_days_given_num_text STRING OPTIONS(description='Number of days drug was given'),
  drug_frequency_num NUMERIC OPTIONS(description='How often a drug is given'),
  treatment_start_date DATE OPTIONS(description='Treatment start date for patient'),
  treatment_end_date DATE OPTIONS(description='Treatment end date for patient'),
  cycle_num_text STRING OPTIONS(description='Two digit Cycle number text'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_id
OPTIONS(
  description='This table contains medical oncology data for the patient'
);
