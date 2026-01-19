-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/eor_amount_stg_other_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service
(
  mon_account_payer_id BIGNUMERIC(38),
  schema_id NUMERIC(29),
  calc_fs_exp_payment NUMERIC(31, 2),
  calc_coinfs_exp_payment NUMERIC(31, 2),
  calc_coinfs_coins_amount NUMERIC(31, 2),
  calc_excl_amount NUMERIC(31, 2),
  calc_irf_teaching_adj NUMERIC(31, 2),
  calc_irf_wage_adj NUMERIC(31, 2),
  calc_irf_lip_adj NUMERIC(31, 2),
  calc_irf_outlier_payment NUMERIC(31, 2),
  tri_drg_payment NUMERIC(35, 6),
  tri_drg_idme_payment NUMERIC(35, 6),
  tri_drg_short_stay_outlier_amt NUMERIC(35, 6),
  tri_drg_transfer_rate NUMERIC(35, 6),
  tri_drg_outlier_amt NUMERIC(35, 6),
  tri_er_exp_payment NUMERIC(31, 2),
  tri_apc_exp_payment NUMERIC(31, 2),
  tri_apc_outlier_payment NUMERIC(31, 2),
  tri_comp_exp_payment NUMERIC(31, 2),
  dw_last_update_date_time DATETIME,
  srv_hcd_exp_payment NUMERIC(31, 2),
  srv_imp_exp_payment NUMERIC(31, 2),
  srv_lab_exp_payment NUMERIC(31, 2),
  srv_lab_service_charges NUMERIC(31, 2),
  srv_ect_exp_payment NUMERIC(31, 2),
  srv_mri_ct_amb_exp_payment NUMERIC(31, 2)
)
CLUSTER BY mon_account_payer_id, schema_id;
