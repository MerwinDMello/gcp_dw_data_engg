-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_account
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Account')
  AS SELECT
      a.account_sid,
      a.account_hier_name,
      a.account_name_parent,
      a.account_name_child,
      a.account_alias_name,
      a.aso_bso_storage_code,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num,
      a.account_uda_name,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.dim_account AS a
  ;
