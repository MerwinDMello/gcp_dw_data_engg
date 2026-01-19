-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account_status
   OPTIONS(description='Dimension table for Account Status')
  AS SELECT
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
