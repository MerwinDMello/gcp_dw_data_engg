-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_bdgt_time.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_bdgt_time
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of a Time within the Year')
  AS SELECT
      a.time_sid,
      a.time_hier_name,
      a.time_name_parent,
      a.time_name_child,
      a.time_alias_name,
      a.aso_bso_storage_code,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num,
      a.time_uda_name,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.dim_bdgt_time AS a
  ;
