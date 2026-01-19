-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_payer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  code STRING NOT NULL,
  contract_id NUMERIC(29),
  ext_payer_id STRING,
  name STRING NOT NULL,
  org_id_payer NUMERIC(29),
  model_covered_population STRING,
  model_product_class STRING,
  date_created DATE NOT NULL,
  date_updated DATE,
  ansi_835_trans_set_id BIGNUMERIC(38),
  calc_engine_contract_code STRING,
  calc_engine_contract_name STRING,
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
  misc_date01 DATE,
  misc_date02 DATE,
  misc_date03 DATE,
  misc_date04 DATE,
  misc_date05 DATE,
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
  major_payer_grp STRING,
  era_prcr_id STRING,
  fin_class_id NUMERIC(29),
  icd10_conversion_date DATE,
  dw_last_update_date DATETIME,
  inpatient_provider_no STRING,
  outpatient_provider_no STRING,
  pyr_type STRING
)
CLUSTER BY id, schema_id;
