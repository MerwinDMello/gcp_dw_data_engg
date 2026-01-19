-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_metric
   OPTIONS(description='Generic Dimension table')
  AS SELECT
      dim_metric.metric_sid,
      dim_metric.metric_hier_name,
      dim_metric.metric_name_parent,
      dim_metric.metric_name_child,
      dim_metric.metric_alias_name,
      dim_metric.aso_bso_storage_code,
      dim_metric.sort_key_num,
      dim_metric.consolidation_code,
      dim_metric.storage_code,
      dim_metric.two_pass_calc_code,
      dim_metric.formula_text,
      dim_metric.member_solve_order_num
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_metric
  ;
