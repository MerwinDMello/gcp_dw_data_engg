-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_ar_sla.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_ar_sla
   OPTIONS(description='This table captures the benchmark value for each facility at financial class code, the AR Aging expected values.')
  AS SELECT
      a.company_code,
      CASE
         a.coid
        WHEN '' THEN 0
        ELSE CAST(a.coid as INT64)
      END AS coid_sid,
      CASE
         c.unit_num
        WHEN '' THEN 0
        ELSE CAST(c.unit_num as INT64)
      END AS unit_num_sid,
      a.coid,
      c.unit_num,
      a.metric_code,
      CASE
         a.financial_class_code
        WHEN '' THEN 0
        ELSE CAST(a.financial_class_code as INT64)
      END AS financial_class_sid,
      a.month_id,
      ROUND(a.metric_value_pct, 5, 'ROUND_HALF_EVEN') AS metric_value_pct,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_ar_sla AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension AS c ON upper(a.coid) = upper(c.coid)
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
