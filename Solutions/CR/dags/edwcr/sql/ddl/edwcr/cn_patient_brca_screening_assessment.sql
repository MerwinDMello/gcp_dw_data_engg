CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_brca_screening_assessment
(
  brca_screening_assessment_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  early_onset_breast_cancer_ind STRING OPTIONS(description='Indicates patient presents with Breast cancer at an age earlier that 50 years of age'),
  ovarian_cancer_history_ind STRING OPTIONS(description='Indicates ovarian cancer in family history.'),
  two_primary_breast_cancer_ind STRING OPTIONS(description='Indicates patient has two primary breast cancers'),
  male_breast_cancer_ind STRING OPTIONS(description='Indicates male breast cancer.'),
  triple_negative_ind STRING OPTIONS(description='Triple negative breast cancer diagnosis means that the offending tumor is estrogen receptor-negative, progesterone receptor-negative and HER2-negative'),
  ashkenazi_jewish_ind STRING OPTIONS(description='Indicates if the patient is of Ashkenazi Jewish decent. They have slightly higher risk for breast cancer.'),
  two_plus_relative_cancer_ind STRING OPTIONS(description='Indicates patient has two or more first or second degree relatives with breast cancer history'),
  meeting_assessment_critieria_ind STRING OPTIONS(description='Indicates the criteria for BRCA screening was met.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, navigator_id, coid
OPTIONS(
  description='Contains the details behind a patients Breast Cancer gene mutation assessment.'
);
