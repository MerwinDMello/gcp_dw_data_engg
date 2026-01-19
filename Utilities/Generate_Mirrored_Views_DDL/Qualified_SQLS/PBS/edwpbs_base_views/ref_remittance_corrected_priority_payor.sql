-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_corrected_priority_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_corrected_priority_payor
   OPTIONS(description='Reference table to maintain th eCorrected Priority Payor details of the claims sent.')
  AS SELECT
      ref_remittance_corrected_priority_payor.corrected_priority_payor_sid,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_qualifier_code,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_id,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_name,
      ref_remittance_corrected_priority_payor.source_system_code,
      ref_remittance_corrected_priority_payor.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_corrected_priority_payor
  ;
