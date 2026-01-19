CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_id_score
(
  cancer_patient_id_output_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each output record.'),
  score_sequence_num INT64 NOT NULL OPTIONS(description='The sequence that the scores were outputed in.'),
  message_control_id_text STRING NOT NULL OPTIONS(description='Unique id for the message provided from EDW'),
  site_associated_model_output_site_desc STRING OPTIONS(description='Variable length array that will include the primary site description identified from the model.'),
  site_associated_model_output_score_num BIGNUMERIC(48, 10) OPTIONS(description='Variable length array that will include the associated model output score for each primary site identified from the model.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_id_output_sk, score_sequence_num
OPTIONS(
  description='Contains the detail scores associated with the cancer id output model on Cloudera.'
);
