-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ra_discrepancy_wrk1.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ra_discrepancy_wrk1
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  patient_dw_id NUMERIC(29) NOT NULL,
  payor_dw_id NUMERIC(29) NOT NULL,
  iplan_insurance_order_num INT64 NOT NULL,
  eor_log_date DATE NOT NULL,
  log_id STRING NOT NULL,
  log_sequence_num INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  unit_num STRING,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_id INT64,
  ar_bill_thru_date DATE,
  remittance_date DATE,
  ra_log_date DATE,
  ra_covered_days_num INT64,
  ra_deductible_coinsurance_amt NUMERIC(32, 3),
  ra_drg_code STRING,
  ra_payment_amt NUMERIC(32, 3),
  ra_payment_date DATE,
  ra_contractual_allowance_amt NUMERIC(32, 3),
  ra_hipps_code STRING,
  ra_total_charge_amt NUMERIC(32, 3),
  ra_non_covered_charge_amt NUMERIC(32, 3),
  ra_gross_reimbursement_amt NUMERIC(32, 3),
  actual_payment_amt NUMERIC(32, 3),
  ra_net_billed_charge_amt NUMERIC(32, 3),
  ra_deductible_amt NUMERIC(32, 3),
  ra_total_deductible_amt NUMERIC(32, 3),
  ra_blood_deductible_amt NUMERIC(32, 3),
  ra_coinsurance_amt NUMERIC(32, 3),
  ra_lab_payment_amt NUMERIC(32, 3),
  ra_therapy_payment_amt NUMERIC(32, 3),
  ra_primary_payor_payment_amt NUMERIC(32, 3),
  ra_total_patient_resp_amt NUMERIC(32, 3),
  ra_copay_amt NUMERIC(32, 3),
  ra_outlier_payment_amt NUMERIC(32, 3),
  ra_capital_payment_amt NUMERIC(32, 3),
  ra_denied_charge_amt NUMERIC(32, 3),
  ra_net_apc_service_amt NUMERIC(32, 3),
  ra_net_fee_schedule_amt NUMERIC(32, 3),
  ra_patient_responsible_amt NUMERIC(32, 3),
  dw_last_update_date_time DATETIME,
  source_system_code STRING NOT NULL
)
CLUSTER BY patient_dw_id;
