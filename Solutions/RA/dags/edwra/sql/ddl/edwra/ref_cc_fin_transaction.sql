-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_fin_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  transaction_code STRING NOT NULL,
  eff_begin_date DATE NOT NULL,
  unit_num STRING,
  transaction_type STRING NOT NULL,
  transaction_desc STRING NOT NULL,
  transaction_category_id NUMERIC(29),
  transaction_master_id NUMERIC(29) NOT NULL,
  inactive_date DATE,
  create_user_id STRING,
  create_date_time DATETIME,
  update_user_id STRING,
  update_date_time DATETIME,
  dw_last_update_date_time DATETIME NOT NULL,
  source_system_code STRING NOT NULL
)
PARTITION BY DATE_TRUNC(eff_begin_date, MONTH)
CLUSTER BY company_code, coid, transaction_code;
