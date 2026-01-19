CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.navque_patient_tumor_driver
(
  navque_patient_tumor_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique Navigation queue key for consolidated patient tumor records'),
  cancer_patient_driver_sk NUMERIC(29) OPTIONS(description='Unique patient key for consolidated patient records'),
  cancer_tumor_driver_sk INT64 OPTIONS(description='A unique identifier for each tumor record'),
  navque_history_id INT64 OPTIONS(description='A unique identifier for each survivorship plan.'),
  navque_action_id INT64 OPTIONS(description='A five character code for each facility.'),
  navque_reason_id INT64 OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each status.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  message_control_id_text STRING OPTIONS(description='Unique Message ID'),
  message_date DATE OPTIONS(description='Date of Message'),
  navque_insert_date DATE OPTIONS(description='Date record was inserted into NavQue'),
  navque_action_date DATE OPTIONS(description='Date Navigator (NavQue user) took action within NavQue'),
  medical_record_num STRING OPTIONS(description='Patient Medical Record Number'),
  patient_market_urn STRING OPTIONS(description='The Market Unique Record Number (MRN) is a unique number to identify a patient across facilities within one Meditech Network -- sometimes loosely called a market -- and across multiple encounters.'),
  network_mnemonic_cs STRING OPTIONS(description='The network mnemonic for the facility in which the patient visited.'),
  transition_of_care_score_num INT64 OPTIONS(description='Patient segmentation Transition of Care score'),
  navigated_patient_ind STRING OPTIONS(description='Field indicating if a patient is being navigated (Navigated, Not Navigated)'),
  message_source_flag STRING OPTIONS(description='Source of Message where D = PT Id and L = NLP'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_driver_sk, coid
OPTIONS(
  description='This tables contain history information for Navigaton Queue'
);
