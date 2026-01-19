-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_same_store_current_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_same_store_current_eom AS SELECT
    CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', current_date('US/Central')), 1, 6)) as INT64) AS month_id,
    date_sub(current_date('US/Central'), interval 1 DAY) AS pe_date,
    a.company_code,
    a.coid,
    'N/A' AS gl_close_ind,
    CASE
      WHEN a.same_store_type_code = 'CSS' THEN 'Y'
      WHEN a.same_store_sub_type_code = 'TSS' THEN 'Y'
      ELSE 'N'
    END AS same_store_ind,
    'EOD' AS day_month_ind,
    'E' AS source_system_code,
    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edw_pub_views.facility AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
UNION ALL
SELECT
    a.month_id,
    DATE(a.dw_last_update_date_time) AS pe_date,
    a.company_code,
    a.coid,
    substr(a.gl_close_ind, 1, 3) AS gl_close_ind,
    substr(a.same_store_ind, 1, 1) AS same_store_ind,
    'EOM' AS day_month_ind,
    substr(a.source_system_code, 1, 1) AS source_system_code,
    a.dw_last_update_date_time AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
