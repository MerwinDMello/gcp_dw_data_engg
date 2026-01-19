-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_ar_sla.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_ar_sla AS SELECT
    a.company_code,
    CAST(bqutil.fn.cw_td_normalize_number(a.coid) as INT64) AS coid_sid,
    CAST(bqutil.fn.cw_td_normalize_number(c.unit_num) as INT64) AS unit_num_sid,
    a.coid,
    c.unit_num,
    a.metric_code,
    CAST(bqutil.fn.cw_td_normalize_number(a.financial_class_code) as INT64) AS financial_class_sid,
    a.month_id,
    a.metric_value_pct,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_ar_sla AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension AS c ON a.coid = c.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
