CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_ed_visit_order_detail
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Unique Identifier assigned by the Data Warehouse.'),
  order_urn STRING NOT NULL OPTIONS(description='Unique Identifier for an order. Unique for a patient at a facility. Patient_DW_Ids across different facilities can have the same Order_URN for different orders.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  order_date_time DATETIME OPTIONS(description='This is the date/time for the order'),
  order_proc_num STRING OPTIONS(description='This is the order procedure number for the lab order'),
  order_proc_mnemonic_cs STRING OPTIONS(description='This is the mnemonic associated with the order procedure.'),
  order_proc_name STRING OPTIONS(description='Description of the order made'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Emergency department data at the order level for a patient. Includes treatments, procedures, and meditcations ordered for a patient.'
);
