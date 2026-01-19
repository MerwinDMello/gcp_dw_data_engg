-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_metric
   OPTIONS(description='Generic Dimension table')
  AS SELECT
      a.metric_sid,
      a.metric_hier_name,
      a.metric_name_parent,
      a.metric_name_child,
      a.metric_alias_name,
      a.aso_bso_storage_code,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_metric AS a
  ;
