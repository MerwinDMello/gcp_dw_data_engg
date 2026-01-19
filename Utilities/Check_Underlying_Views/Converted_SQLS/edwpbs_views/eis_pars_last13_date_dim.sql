-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_pars_last13_date_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_pars_last13_date_dim AS SELECT
    a.time_id AS last_13_time_id,
    concat('Last13Month_', substr(CAST(CAST(CASE
      WHEN CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) > CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) THEN 12 * (CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC) - CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 0, 'ROUND_HALF_EVEN') as NUMERIC))
      ELSE CAST(0 as NUMERIC)
    END + mod(CAST(ROUND(ROUND(a.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 2, 'ROUND_HALF_EVEN') as NUMERIC), 1) * 100 - mod(CAST(ROUND(ROUND(b.time_id / NUMERIC '100.0', 2, 'ROUND_HALF_EVEN'), 2, 'ROUND_HALF_EVEN') as NUMERIC), 1) * 100 + 1 as INT64) as STRING), 1, 2)) AS last_13_month_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS b
  WHERE b.time_id = (
    SELECT
        min(eis_date_dim.time_id)
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE upper(eis_date_dim_1.last_13_mth_flag) = 'Y'
  )
   AND upper(b.last_13_mth_flag) = 'Y'
   AND upper(a.last_13_mth_flag) = 'Y'
;
