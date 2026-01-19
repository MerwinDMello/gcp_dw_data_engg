-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_remittance_advice_stg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  payor_dw_id NUMERIC(29) NOT NULL,
  remittance_advice_num INT64 NOT NULL,
  remittance_header_id BIGNUMERIC(38) NOT NULL,
  unit_num STRING,
  iplan_id INT64,
  payment_date DATE,
  remittance_date DATE,
  remittance_amt NUMERIC(32, 3),
  check_num STRING,
  icn_num STRING,
  group_control_num STRING,
  create_date_time DATETIME,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, coid, payor_dw_id, remittance_advice_num;
