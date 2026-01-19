CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_imaging
(
  cn_patient_imaging_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  imaging_type_id INT64 OPTIONS(description='A unique identifier for each image type.'),
  imaging_mode_id INT64 OPTIONS(description='A unique identifier for the different modes the image can be taken with.'),
  imaging_area_side_id INT64 OPTIONS(description='Which side of the body the image was taken.'),
  imaging_facility_id INT64 OPTIONS(description='The facility in which the image was taken,'),
  disease_status_id INT64 OPTIONS(description='Identifier of disease status at the time of imaging'),
  treatment_status_id INT64 OPTIONS(description='Identifier of treatment status at the time of imaging\r'),
  core_record_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each core record type.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  imaging_date DATE OPTIONS(description='The date the image was taken.'),
  imaging_location_text STRING OPTIONS(description='A free text field that identifies which part of the body the image was taken.'),
  birad_scale_code STRING OPTIONS(description='A scale mammogram screening (for breast cancer diagnosis) into a small number of well-defined categories'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  other_image_type_text STRING OPTIONS(description='Text for other image types'),
  initial_diagnosis_ind STRING OPTIONS(description='Indicator if the Imaging is related to initial disease\r'),
  disease_monitoring_ind STRING OPTIONS(description='Indicator if the Imaging is related to disease monitoring'),
  radiology_result_text STRING OPTIONS(description='Text field captures result for radiology'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, med_spcl_physician_id, tumor_type_id, diagnosis_result_id
OPTIONS(
  description='Contains the details behind the imaging of a patient.'
);
