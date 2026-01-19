-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account AS SELECT
    dim_account.account_sid,
    dim_account.account_hier_name,
    dim_account.account_name_parent,
    dim_account.account_name_child,
    dim_account.account_alias_name,
    dim_account.aso_bso_storage_code,
    dim_account.sort_key_num,
    dim_account.consolidation_code,
    dim_account.storage_code,
    dim_account.two_pass_calc_code,
    dim_account.formula_text,
    dim_account.member_solve_order_num,
    dim_account.account_uda_name,
    dim_account.source_system_code,
    dim_account.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_account
;
