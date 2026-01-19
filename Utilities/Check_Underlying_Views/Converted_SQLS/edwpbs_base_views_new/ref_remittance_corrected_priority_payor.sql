-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_corrected_priority_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_corrected_priority_payor AS SELECT
    ref_remittance_corrected_priority_payor.corrected_priority_payor_sid,
    ref_remittance_corrected_priority_payor.corrected_priority_payor_qualifier_code,
    ref_remittance_corrected_priority_payor.corrected_priority_payor_id,
    ref_remittance_corrected_priority_payor.corrected_priority_payor_name,
    ref_remittance_corrected_priority_payor.source_system_code,
    ref_remittance_corrected_priority_payor.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_corrected_priority_payor
;
