-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_level
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels in a denial resolution process it was Appealed.')
  AS SELECT
      dim_appeal_level.appeal_level_sid,
      dim_appeal_level.aso_bso_storage_code,
      dim_appeal_level.appeal_level_name_parent,
      dim_appeal_level.appeal_level_name_child,
      dim_appeal_level.appeal_level_alias_name,
      dim_appeal_level.sort_key_num,
      dim_appeal_level.two_pass_calc_code,
      dim_appeal_level.consolidation_code,
      dim_appeal_level.storage_code,
      dim_appeal_level.formula_text,
      dim_appeal_level.member_solve_order_num,
      dim_appeal_level.appeal_level_hier_name
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.dim_appeal_level
  ;
