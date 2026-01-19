-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ssi_unbilled_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ssi_unbilled_status AS SELECT
    ref_ssi_unbilled_status.ssi_unbilled_status_code,
    ref_ssi_unbilled_status.ssi_unbilled_status_desc,
    ref_ssi_unbilled_status.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_ssi_unbilled_status
;
