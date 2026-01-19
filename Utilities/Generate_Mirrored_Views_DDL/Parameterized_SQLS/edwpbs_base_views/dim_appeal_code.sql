-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_appeal_code
   OPTIONS(description='Dimension table that captures the hierarchy and descriptions of various Appeals in the denial resolution process')
  AS SELECT
      dim_appeal_code.appeal_code_sid,
      dim_appeal_code.aso_bso_storage_code,
      dim_appeal_code.appeal_code_parent,
      dim_appeal_code.appeal_code_child,
      dim_appeal_code.appeal_code_alias_name,
      dim_appeal_code.sort_key_num,
      dim_appeal_code.two_pass_calc_code,
      dim_appeal_code.consolidation_code,
      dim_appeal_code.storage_code,
      dim_appeal_code.formula_text,
      dim_appeal_code.member_solve_order_num,
      dim_appeal_code.appeal_code_hier_name,
      dim_appeal_code.appeal_code_uda_name
    FROM
      {{ params.param_pbs_core_dataset_name }}.dim_appeal_code
  ;
