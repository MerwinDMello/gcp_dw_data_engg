-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_time.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_bdgt_time
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of a Time within the Year')
  AS SELECT
      dim_bdgt_time.time_sid,
      dim_bdgt_time.time_hier_name,
      dim_bdgt_time.time_name_parent,
      dim_bdgt_time.time_name_child,
      dim_bdgt_time.time_alias_name,
      dim_bdgt_time.aso_bso_storage_code,
      dim_bdgt_time.sort_key_num,
      dim_bdgt_time.consolidation_code,
      dim_bdgt_time.storage_code,
      dim_bdgt_time.two_pass_calc_code,
      dim_bdgt_time.formula_text,
      dim_bdgt_time.member_solve_order_num,
      dim_bdgt_time.time_uda_name,
      dim_bdgt_time.source_system_code,
      dim_bdgt_time.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.dim_bdgt_time
  ;
