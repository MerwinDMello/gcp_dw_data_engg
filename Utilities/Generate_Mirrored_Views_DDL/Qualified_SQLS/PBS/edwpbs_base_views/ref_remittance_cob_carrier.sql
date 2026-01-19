-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_cob_carrier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_cob_carrier
   OPTIONS(description='Reference table to maintain the Coordination Of Benefit Carrier details of the claims sent.')
  AS SELECT
      ref_remittance_cob_carrier.cob_carrier_sid,
      ref_remittance_cob_carrier.cob_qualifier_code,
      ref_remittance_cob_carrier.cob_carrier_num,
      ref_remittance_cob_carrier.cob_carrier_name,
      ref_remittance_cob_carrier.source_system_code,
      ref_remittance_cob_carrier.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_cob_carrier
  ;
