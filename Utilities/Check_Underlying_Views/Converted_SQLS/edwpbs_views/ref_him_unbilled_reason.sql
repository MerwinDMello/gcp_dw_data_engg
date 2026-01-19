-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_him_unbilled_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_him_unbilled_reason AS SELECT
    ref_him_unbilled_reason.unbilled_reason_code,
    ref_him_unbilled_reason.unbilled_reason_desc,
    ref_him_unbilled_reason.unbilled_reason_long_desc,
    ref_him_unbilled_reason.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_him_unbilled_reason
;
