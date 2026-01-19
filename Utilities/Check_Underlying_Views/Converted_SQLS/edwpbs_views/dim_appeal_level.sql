-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_appeal_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_appeal_level
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels in a denial resolution process it was Appealed.')
  AS SELECT
      a.appeal_level_sid,
      a.aso_bso_storage_code,
      a.appeal_level_name_parent,
      a.appeal_level_name_child,
      a.appeal_level_alias_name,
      a.sort_key_num,
      a.two_pass_calc_code,
      a.consolidation_code,
      a.storage_code,
      a.formula_text,
      a.member_solve_order_num,
      a.appeal_level_hier_name
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_level AS a
  ;
