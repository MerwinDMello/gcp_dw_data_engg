-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_same_store_current_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_same_store_current_eom AS SELECT
    CASE
       format_date('%Y%m', current_date('US/Central'))
      WHEN '' THEN 0
      ELSE CAST(format_date('%Y%m', current_date('US/Central')) as INT64)
    END AS month_id,
    date_sub(current_date('US/Central'), interval 1 DAY) AS pe_date,
    a.company_code,
    a.coid,
    'N/A' AS gl_close_ind,
    CASE
      WHEN upper(a.same_store_type_code) = 'CSS' THEN 'Y'
      WHEN upper(a.same_store_sub_type_code) = 'TSS' THEN 'Y'
      ELSE 'N'
    END AS same_store_ind,
    'EOD' AS day_month_ind,
    'E' AS source_system_code,
    timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edw_pub_views.facility AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
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
    CAST(a.dw_last_update_date_time as TIMESTAMP) AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
;
