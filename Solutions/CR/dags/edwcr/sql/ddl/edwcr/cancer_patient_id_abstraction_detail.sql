CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction_detail
(
  cancer_abstraction_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each output record.'),
  abstraction_measure_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each measure in abstraction'),
  cancer_patient_id_output_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each output record.'),
  message_control_id_text STRING OPTIONS(description='Unique id for the message provided from EDW'),
  coid STRING OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  patient_dw_id NUMERIC(29) OPTIONS(description='An internal EDW identifier for each encounter.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='Account Number for the message'),
  predicted_value_text STRING OPTIONS(description='Text for Sarcoma predicted'),
  submitted_value_text STRING OPTIONS(description='Text for Sarcoma by submitted primary site'),
  suggested_value_text STRING OPTIONS(description='Text for Sarcoma by suggested primary site'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='This table contains the abstraction detail data of the cancer patient identifier model on HDFS.'
);
