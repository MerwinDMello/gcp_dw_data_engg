-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_metric AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.dim_metric
;
