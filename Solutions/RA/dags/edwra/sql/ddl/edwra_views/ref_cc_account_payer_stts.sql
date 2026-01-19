-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_account_payer_stts.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_account_payer_stts AS SELECT
    a.company_code,
    a.status_id,
    a.status_category_id,
    a.status_name,
    a.status_desc,
    a.status_phase_id,
    a.probability_pct,
    a.incl_new_acct_ind,
    a.pvnt_acct_ovrd_ind,
    a.acct_level_assn_ind,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_account_payer_stts AS a
;
