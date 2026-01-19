-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_source.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_source
   OPTIONS(description='Dimension table that captures the Source systems currently used in PARS hierarchy')
  AS SELECT
      a.source_sid,
      a.source_hier_name,
      a.aso_bso_storage_code,
      a.source_parent,
      a.source_child,
      a.source_alias_name,
      a.sort_key_num,
      a.two_pass_calc_code,
      a.consolidation_code,
      a.storage_code,
      a.formula_text,
      a.member_solve_order_num,
      a.source_uda_name,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.dim_source AS a
  ;
