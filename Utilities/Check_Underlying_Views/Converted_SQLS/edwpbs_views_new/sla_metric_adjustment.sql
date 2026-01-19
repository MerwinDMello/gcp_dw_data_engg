-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sla_metric_adjustment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sla_metric_adjustment AS SELECT
    s.service_type_name,
    a.corporate_code,
    c.parent_code,
    a.child_code,
    c.fact_lvl_code,
    a.month_id,
    a.metric_code,
    a.metric_sub_code,
    a.metric_value
  FROM
    (
      SELECT
          sla_metric_adjustment.service_type_name,
          sla_metric_adjustment.corporate_code,
          sla_metric_adjustment.parent_code,
          sla_metric_adjustment.child_code,
          sla_metric_adjustment.fact_lvl_code,
          sla_metric_adjustment.month_id,
          sla_metric_adjustment.metric_code,
          sla_metric_adjustment.metric_sub_code,
          sla_metric_adjustment.metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.sla_metric_adjustment
    ) AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_service_type AS s
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS c ON upper(s.service_type_name) = upper(c.service_type_name)
     AND c.coid = a.child_code
     AND c.fact_lvl_code IN(
      3, 4, 5, 7
    )
;
