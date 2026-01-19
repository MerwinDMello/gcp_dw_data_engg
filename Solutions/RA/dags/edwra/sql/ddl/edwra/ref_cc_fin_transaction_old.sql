-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_fin_transaction_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction_old
(
  company_code STRING NOT NULL OPTIONS(description='A one character code that identifies a specific company or Coid.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  transaction_code STRING NOT NULL OPTIONS(description='An eight digit code that identifies a specific financial transaction.'),
  eff_begin_date DATE NOT NULL OPTIONS(description='The effective date for a given transaction.'),
  unit_num STRING OPTIONS(description='Secondary identifier for a given facility or Coid.'),
  transaction_type STRING NOT NULL OPTIONS(description='Code indicating the transaction type (P = payment, A = adjustment, V = valid value'),
  transaction_desc STRING NOT NULL OPTIONS(description='The description of the payment or adjustment code.'),
  transaction_category_id NUMERIC(29) OPTIONS(description='Indicates the transaction category for an appeal transaction.'),
  transaction_master_id NUMERIC(29) NOT NULL OPTIONS(description='Primary identifier for a given transaction master row in relation to an appeal transaction.'),
  inactive_date DATE OPTIONS(description='The date when a transaction becomes inactive on the Concuity system.'),
  create_user_id STRING OPTIONS(description='The Concuity end-user that created a transaction record.'),
  create_date_time DATETIME OPTIONS(description='The date and time of the record creation.'),
  update_user_id STRING OPTIONS(description='The Concuity end-user that modified a transaction record.'),
  update_date_time DATETIME OPTIONS(description='The date and time of the record modification.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='The last update date and time row was commited to the EDW.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
PARTITION BY DATE_TRUNC(eff_begin_date, MONTH)
CLUSTER BY company_code, coid, transaction_code
OPTIONS(
  description='Reference Concuity Financial Transaction'
);
