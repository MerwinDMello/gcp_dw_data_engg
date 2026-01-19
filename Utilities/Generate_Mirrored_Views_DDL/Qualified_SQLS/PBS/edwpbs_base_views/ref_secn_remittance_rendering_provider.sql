-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_secn_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_secn_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider Secondary Information of the Claims recevied.')
  AS SELECT
      ROUND(ref_secn_remittance_rendering_provider.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      ref_secn_remittance_rendering_provider.secn_rendering_provider_id_qlfr_code,
      ref_secn_remittance_rendering_provider.secn_rendering_provider_id,
      ref_secn_remittance_rendering_provider.source_system_code,
      ref_secn_remittance_rendering_provider.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_secn_remittance_rendering_provider
  ;
