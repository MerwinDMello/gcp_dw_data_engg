CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_imaging_result_detail
(
  clinical_finding_sk NUMERIC(29) NOT NULL OPTIONS(description='This element is the anchor surrogate key of the clinical finding'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  image_occurence_ts DATETIME OPTIONS(description='The Imaging Occurred TS is a valid calendar date together with a valid hours, minutes and seconds when the finding was observed or was performed.'),
  source_system_original_code STRING OPTIONS(description='This is the code as it appears in the originating system before any mapping or tranformation has been performed.'),
  imaging_type_code STRING OPTIONS(description='Contains the type of imaging that they are using to perform the exam'),
  procedure_mnemonic_cs STRING OPTIONS(description='This is the procedure Mnemonic'),
  rad_exam_name STRING OPTIONS(description='Description of the Exam'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains the imaging result'
);
