-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Account')
  AS SELECT
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
