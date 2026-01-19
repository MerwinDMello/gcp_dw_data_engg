-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_cc_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_cc_reason AS SELECT
    ref_cc_reason.company_code,
    ref_cc_reason.cc_reason_id,
    ref_cc_reason.cc_reason_long_desc,
    ref_cc_reason.cc_reason_short_desc,
    ref_cc_reason.pa_reason_code,
    ref_cc_reason.active_ind,
    ref_cc_reason.dw_last_update_date_time,
    ref_cc_reason.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_cc_reason
;
