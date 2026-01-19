-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_sla_all_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_all_level AS SELECT
    aa.month_id,
    max(aa.service_type_name) AS service_type_name,
    max(aa.corporate_code) AS corporate_code,
    cc.fact_lvl_code,
    max(cc.parent_code) AS parent_code,
    max(cc.child_code) AS child_code,
    max(aa.metric_code) AS metric_code,
    max(aa.metric_type_code) AS metric_type_code,
    sum(aa.pass_count) AS pass_count,
    sum(aa.fail_count) AS fail_count
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS bb
    INNER JOIN (
      SELECT
          a.month_id,
          max(a.service_type_name) AS service_type_name,
          a.fact_lvl_code,
          max(a.corporate_code) AS corporate_code,
          max(a.parent_code) AS parent_code,
          max(a.metric_code) AS metric_code,
          max(a.metric_type_code) AS metric_type_code,
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
           AND upper(a.service_type_name) = upper(b.service_type_name)
           AND a.fact_lvl_code = b.fact_lvl_code
           AND upper(a.corporate_code) = upper(b.corporate_code)
           AND upper(a.parent_code) = upper(b.parent_code)
           AND upper(a.child_code) = upper(b.child_code)
           AND upper(a.metric_code) = upper(b.metric_code)
           AND upper(a.status_flag) = upper(b.status_flag)
        GROUP BY 1, upper(a.service_type_name), 3, upper(a.corporate_code), upper(a.parent_code), upper(a.metric_code), upper(a.metric_type_code)
    ) AS aa ON upper(aa.service_type_name) = upper(bb.service_type_name)
     AND upper(aa.parent_code) = upper(bb.coid)
     AND bb.fact_lvl_code IN(
      3, 4, 5, 1, 2, 6
    )
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS cc ON upper(aa.service_type_name) = upper(cc.service_type_name)
     AND upper(bb.parent_code) = upper(cc.child_code)
  GROUP BY 1, upper(aa.service_type_name), upper(aa.corporate_code), 4, upper(cc.parent_code), upper(cc.child_code), upper(aa.metric_code), upper(aa.metric_type_code)
;
