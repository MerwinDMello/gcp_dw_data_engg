-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/momon_account_calc_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_calc_detail
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_payer_id BIGNUMERIC(38) NOT NULL,
  display_sequence NUMERIC(29) NOT NULL,
  amount NUMERIC(31, 2) NOT NULL,
  mon_payer_id BIGNUMERIC(38) NOT NULL,
  rate NUMERIC(33, 4),
  service_description STRING NOT NULL,
  service_type STRING NOT NULL, 
  units NUMERIC(30, 1),
  date_created DATETIME NOT NULL,
  date_updated DATETIME,
  misc_char01 STRING,
  misc_char02 STRING,
  misc_char03 STRING,
  misc_char04 STRING,
  misc_char05 STRING,
  misc_char06 STRING,
  misc_char07 STRING,
  misc_char08 STRING,
  misc_char09 STRING,
  misc_char10 STRING,
  misc_date01 DATETIME,
  misc_date02 DATETIME,
  misc_date03 DATETIME,
  misc_date04 DATETIME,
  misc_date05 DATETIME,
  misc_amt01 NUMERIC(31, 2),
  misc_amt02 NUMERIC(31, 2),
  misc_amt03 NUMERIC(31, 2),
  misc_amt04 NUMERIC(31, 2),
  misc_amt05 NUMERIC(31, 2),
  misc_yn01 STRING,
  misc_yn02 STRING,
  misc_yn03 STRING,  
  misc_yn04 STRING,
  misc_yn05 STRING,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
