-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_cc_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_cc_reason AS SELECT
    bv.company_code,
    bv.cc_reason_id,
    bv.cc_reason_long_desc,
    bv.cc_reason_short_desc,
    bv.pa_reason_code,
    bv.active_ind,
    bv.dw_last_update_date_time,
    bv.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_cc_reason AS bv
    INNER JOIN (
      SELECT
          max(secref_facility.company_code) AS company_code,
          max(secref_facility.user_id) AS user_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility
        WHERE secref_facility.user_id = session_user()
        GROUP BY upper(secref_facility.company_code), upper(secref_facility.user_id)
    ) AS sf ON upper(bv.company_code) = upper(sf.company_code)
     AND sf.user_id = session_user()
;
