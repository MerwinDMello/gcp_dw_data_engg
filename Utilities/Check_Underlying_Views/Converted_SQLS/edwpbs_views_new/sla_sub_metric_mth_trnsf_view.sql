-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sla_sub_metric_mth_trnsf_view.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sla_sub_metric_mth_trnsf_view AS SELECT
    k.corporate_code,
    k.parent_month_id,
    k.metric_code,
    k.metric_sub_code,
    lu_month.month_id AS child_month_id,
    k.p_month_id_desc_s AS parent_month_desc,
    lu_month.month_id_desc_s AS child_month_desc
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month
    INNER JOIN (
      SELECT
          a.month_id AS parent_month_id,
          a.month_id_desc_s AS p_month_id_desc_s,
          b.metric_code,
          b.metric_sub_code,
          b.corporate_code,
          substr(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval -(CAST(/* expression of unknown or erroneous type */ b.metric_range_value as INT64) - 1) MONTH)), 1, 6) AS child_month_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a
          CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_sub_metric AS b
        WHERE a.month_id <= CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', current_date('US/Central')), 1, 6)) as FLOAT64)
    ) AS k ON lu_month.month_id BETWEEN CAST(bqutil.fn.cw_td_normalize_number(k.child_month_id) as INT64) AND k.parent_month_id
  WHERE k.parent_month_id >= CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', date_add(current_date('US/Central'), interval -36 MONTH)), 1, 6)) as FLOAT64)
;
