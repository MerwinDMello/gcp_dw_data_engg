-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account_status AS SELECT
    dim_account_status.account_status_sid,
    dim_account_status.aso_bso_storage_code,
    dim_account_status.account_status_name_parent,
    dim_account_status.account_status_name_child,
    dim_account_status.account_status_child_alias_name,
    dim_account_status.alias_table_name,
    dim_account_status.sort_key_num,
    dim_account_status.consolidation_code,
    dim_account_status.storage_code,
    dim_account_status.two_pass_calc_code,
    dim_account_status.formula_text,
    dim_account_status.member_solve_order_num,
    dim_account_status.account_status_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_account_status
;
