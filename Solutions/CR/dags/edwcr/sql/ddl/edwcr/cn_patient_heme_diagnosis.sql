CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_diagnosis
(
  cn_patient_heme_diagnosis_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  disease_status_id INT64 OPTIONS(description='Identifier for disease status of the hematology disease '),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  speciman_date DATE OPTIONS(description='Date of Specimen resulting in Diagnosis'),
  disease_diagnosis_text STRING OPTIONS(description='Text for hematology disease diagnosis type  level 1'),
  therapy_related_ind STRING OPTIONS(description='Indication of therapy relation to disease diagnosis'),
  transformed_from_mds_ind STRING OPTIONS(description='Indicates the disease that transformed from MDS'),
  mipi_text STRING OPTIONS(description='Value for Mantle cell lymphoma prognostic index'),
  ipi_text STRING OPTIONS(description='Value for International prognostic index'),
  flipi_text STRING OPTIONS(description='Value for Follicular Lymphoma prognostic index'),
  aids_related_ind STRING OPTIONS(description='Indication that the disease diagnosis is AIDS related'),
  comment_text STRING OPTIONS(description='Disease diagnosis navigator free text comments'),
  classification_text STRING OPTIONS(description='Text for diagnosis classification�'),
  sub_classification_text STRING OPTIONS(description='Text for diagnosis sub classification�'),
  nhl_type_text STRING OPTIONS(description='Text for Non-Hodgkin Lymphoma '),
  other_nhl_type_text STRING OPTIONS(description='Text for other type of Non-Hodgkin Lymphoma'),
  transformed_disease_text STRING OPTIONS(description='Text for transformed disease'),
  non_malignant_type_text STRING OPTIONS(description='Text for non malignant type'),
  feature_text STRING OPTIONS(description='Text for feature of disease diagnosis'),
  risk_category_text STRING OPTIONS(description='Text for risk category'),
  mds_disease_risk_text STRING OPTIONS(description='Text for Myelodysplastic Syndromes Disease Risk '),
  staging_field_1_text STRING OPTIONS(description='Text for patient stage field 1�'),
  staging_field_2_text STRING OPTIONS(description='Text for patient stage field 2�'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains diagnosis details for Hematology patient'
);
