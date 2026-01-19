-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/edw_remit_recon_aggregated.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.edw_remit_recon_aggregated
(
  unit_num STRING NOT NULL,
  ra_pat_acct_num INT64,
  ra_covered_days_num INT64,
  ra_visit_cnt INT64,
  ra_covered_days_visit_cnt INT64,
  ra_non_replaced_blood_unit_qty INT64,
  ra_prescription_qty INT64,
  ra_total_charge_amt NUMERIC(32, 3),
  ra_non_covered_charge_amt NUMERIC(32, 3),
  ra_net_billed_charge_amt NUMERIC(32, 3),
  ra_deductible_amt NUMERIC(32, 3),
  ra_coinsurance_amt NUMERIC(32, 3),
  ra_net_benefit_amt NUMERIC(32, 3),
  ra_insurance_payment_amt NUMERIC(32, 3),
  ra_patient_responsible_amt NUMERIC(32, 3),
  schema_id INT64 NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY unit_num, schema_id;
