-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_bdgt_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_bdgt_year
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Year')
  AS SELECT
      a.year_sid,
      a.year_hier_name,
      a.year_name_parent,
      a.year_name_child,
      a.year_alias_name,
      a.aso_bso_storage_code,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num,
      a.year_uda_name,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_bdgt_year AS a
  ;
