-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
