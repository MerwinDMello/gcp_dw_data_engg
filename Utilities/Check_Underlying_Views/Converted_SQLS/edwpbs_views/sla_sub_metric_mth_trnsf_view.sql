-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
          format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval -(CASE
             b.metric_range_value
            WHEN '' THEN 0
            ELSE CAST(b.metric_range_value as INT64)
          END - 1) MONTH)) AS child_month_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a
          CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_sub_metric AS b
        WHERE a.month_id <= CASE
           format_date('%Y%m', current_date('US/Central'))
          WHEN '' THEN 0.0
          ELSE CAST(format_date('%Y%m', current_date('US/Central')) as FLOAT64)
        END
    ) AS k ON lu_month.month_id BETWEEN CASE
       k.child_month_id
      WHEN '' THEN 0
      ELSE CAST(k.child_month_id as INT64)
    END AND k.parent_month_id
  WHERE k.parent_month_id >= CASE
     format_date('%Y%m', date_add(current_date('US/Central'), interval -36 MONTH))
    WHEN '' THEN 0.0
    ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -36 MONTH)) as FLOAT64)
  END
;
