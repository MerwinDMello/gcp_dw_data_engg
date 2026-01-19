-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_account_payer_stts.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_account_payer_stts AS SELECT
    ref_cc_account_payer_stts.company_code,
    ref_cc_account_payer_stts.status_id,
    ref_cc_account_payer_stts.status_category_id,
    ref_cc_account_payer_stts.status_name,
    ref_cc_account_payer_stts.status_desc,
    ref_cc_account_payer_stts.status_phase_id,
    ref_cc_account_payer_stts.probability_pct,
    ref_cc_account_payer_stts.incl_new_acct_ind,
    ref_cc_account_payer_stts.pvnt_acct_ovrd_ind,
    ref_cc_account_payer_stts.acct_level_assn_ind,
    ref_cc_account_payer_stts.dw_last_update_date_time,
    ref_cc_account_payer_stts.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_account_payer_stts
;
