-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
          secref_facility.company_code,
          max(secref_facility.user_id) AS user_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility
        WHERE secref_facility.user_id = session_user()
        GROUP BY 1, upper(secref_facility.user_id)
    ) AS sf ON bv.company_code = sf.company_code
     AND sf.user_id = session_user()
;
