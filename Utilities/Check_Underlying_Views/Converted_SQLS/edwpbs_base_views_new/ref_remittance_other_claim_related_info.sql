-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_other_claim_related_info AS SELECT
    ref_remittance_other_claim_related_info.ref_sid,
    ref_remittance_other_claim_related_info.reference_id_qualifier_code,
    ref_remittance_other_claim_related_info.reference_id,
    ref_remittance_other_claim_related_info.source_system_code,
    ref_remittance_other_claim_related_info.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_other_claim_related_info
;
