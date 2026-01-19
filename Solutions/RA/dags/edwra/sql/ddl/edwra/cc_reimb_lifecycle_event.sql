-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_reimb_lifecycle_event.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL OPTIONS(description=' The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL,
  payor_dw_id NUMERIC(29) NOT NULL,
  iplan_insurance_order_num INT64 NOT NULL,
  eor_log_date DATE NOT NULL,
  log_id STRING NOT NULL,
  log_sequence_num INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  reimb_lifecycle_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' Expanded column from 18 to 32 positions.'),
  unit_num STRING NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_id INT64 NOT NULL,
  lifecycle_date DATE NOT NULL,
  expected_payment_amt NUMERIC(32, 3),
  actual_payment_amt NUMERIC(32, 3),
  payor_due_amt NUMERIC(32, 3),
  lifecycle_event_type_id NUMERIC(29) NOT NULL,
  account_payer_status_id NUMERIC(29),
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
PARTITION BY DATE_TRUNC(eor_log_date, MONTH)
CLUSTER BY company_code, coid, patient_dw_id, payor_dw_id;
