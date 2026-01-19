-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_pars_last_date_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_pars_last_date_dim AS SELECT
    a.time_id AS last_24_time_id,
    concat('Last24Month_', substr(CAST(CAST(CASE
      WHEN CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) > CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) THEN 12 * (CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) - CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC))
      ELSE CAST(0 as NUMERIC)
    END + mod(CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 2, 'ROUND_HALF_EVEN') as NUMERIC), 1) * 100 - mod(CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 2, 'ROUND_HALF_EVEN') as NUMERIC), 1) * 100 + 1 as INT64) as STRING), 1, 2)) AS last_24_month_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS b
  WHERE b.time_id = (
    SELECT
        min(eis_date_dim.time_id)
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
      WHERE eis_date_dim.last24_flag = 'Y'
  )
   AND b.last24_flag = 'Y'
   AND a.last24_flag = 'Y'
;
