-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_account_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_account_status
   OPTIONS(description='Dimension table for Account Status')
  AS SELECT
      a.account_status_sid,
      a.aso_bso_storage_code,
      a.account_status_name_parent,
      a.account_status_name_child,
      a.account_status_child_alias_name AS account_status_child_alias,
      a.alias_table_name,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num,
      a.account_status_hier_name
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.dim_account_status AS a
  ;
