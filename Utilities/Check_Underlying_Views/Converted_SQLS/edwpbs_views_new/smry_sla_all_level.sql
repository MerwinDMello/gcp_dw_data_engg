-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_sla_all_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_all_level AS SELECT
    aa.month_id,
    aa.service_type_name,
    aa.corporate_code,
    cc.fact_lvl_code,
    max(cc.parent_code) AS parent_code,
    max(cc.child_code) AS child_code,
    aa.metric_code,
    aa.metric_type_code,
    sum(aa.pass_count) AS pass_count,
    sum(aa.fail_count) AS fail_count
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS bb
    INNER JOIN (
      SELECT
          a.month_id,
          a.service_type_name,
          a.fact_lvl_code,
          a.corporate_code,
          a.parent_code,
          a.metric_code,
          a.metric_type_code,
          sum(CASE
            WHEN a.result_ind = 1
             AND upper(a.status_flag) = 'Y' THEN 1
            ELSE 0
          END) AS pass_count,
          sum(CASE
            WHEN b.result_ind = 0
             AND upper(a.status_flag) = 'Y' THEN 1
            ELSE 0
          END) AS fail_count
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_month_scorecard AS a
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_month_scorecard AS b ON a.month_id = b.month_id
           AND a.service_type_name = b.service_type_name
           AND a.fact_lvl_code = b.fact_lvl_code
           AND a.corporate_code = b.corporate_code
           AND a.parent_code = b.parent_code
           AND a.child_code = b.child_code
           AND a.metric_code = b.metric_code
           AND a.status_flag = b.status_flag
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    ) AS aa ON upper(aa.service_type_name) = upper(bb.service_type_name)
     AND aa.parent_code = bb.coid
     AND bb.fact_lvl_code IN(
      3, 4, 5, 1, 2, 6
    )
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS cc ON upper(aa.service_type_name) = upper(cc.service_type_name)
     AND bb.parent_code = cc.child_code
  GROUP BY 1, 2, 3, 4, upper(cc.parent_code), upper(cc.child_code), 7, 8
;
