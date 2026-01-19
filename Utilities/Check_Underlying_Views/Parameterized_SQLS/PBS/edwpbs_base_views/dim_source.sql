-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_source.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_source
   OPTIONS(description='Dimension table that captures the Source systems currently used in PARS hierarchy')
  AS SELECT
      dim_source.source_sid,
      dim_source.source_hier_name,
      dim_source.aso_bso_storage_code,
      dim_source.source_parent,
      dim_source.source_child,
      dim_source.source_alias_name,
      dim_source.sort_key_num,
      dim_source.two_pass_calc_code,
      dim_source.consolidation_code,
      dim_source.storage_code,
      dim_source.formula_text,
      dim_source.member_solve_order_num,
      dim_source.source_uda_name,
      dim_source.source_system_code,
      dim_source.dw_last_update_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.dim_source
  ;
