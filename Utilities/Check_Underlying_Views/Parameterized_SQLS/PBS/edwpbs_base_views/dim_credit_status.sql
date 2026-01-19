-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_credit_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_credit_status
   OPTIONS(description='Dimension table for Credit Status')
  AS SELECT
      dim_credit_status.credit_status_sid,
      dim_credit_status.aso_bso_storage_code,
      dim_credit_status.credit_status_name_parent,
      dim_credit_status.credit_status_name_child,
      dim_credit_status.credit_status_child_alias_name,
      dim_credit_status.alias_table_name,
      dim_credit_status.sort_key_num,
      dim_credit_status.consolidation_code,
      dim_credit_status.storage_code,
      dim_credit_status.two_pass_calc_code,
      dim_credit_status.formula_text,
      dim_credit_status.member_solve_order_num,
      dim_credit_status.credit_status_hier_name
    FROM
      {{ params.param_pbs_core_dataset_name }}.dim_credit_status
  ;
