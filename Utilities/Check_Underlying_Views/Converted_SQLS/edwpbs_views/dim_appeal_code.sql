-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_appeal_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_appeal_code
   OPTIONS(description='Dimension table that captures the hierarchy and descriptions of various Appeals in the denial resolution process')
  AS SELECT
      a.appeal_code_sid,
      a.aso_bso_storage_code,
      a.appeal_code_parent,
      a.appeal_code_child,
      a.appeal_code_alias_name,
      a.sort_key_num,
      a.two_pass_calc_code,
      a.consolidation_code,
      a.storage_code,
      a.formula_text,
      a.member_solve_order_num,
      a.appeal_code_hier_name,
      a.appeal_code_uda_name
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_code AS a
  ;
