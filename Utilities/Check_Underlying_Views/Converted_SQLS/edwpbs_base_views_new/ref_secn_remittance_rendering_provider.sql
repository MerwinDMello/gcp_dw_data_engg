-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_secn_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_secn_remittance_rendering_provider AS SELECT
    ref_secn_remittance_rendering_provider.remittance_secn_rendering_provider_sid,
    ref_secn_remittance_rendering_provider.secn_rendering_provider_id_qlfr_code,
    ref_secn_remittance_rendering_provider.secn_rendering_provider_id,
    ref_secn_remittance_rendering_provider.source_system_code,
    ref_secn_remittance_rendering_provider.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_secn_remittance_rendering_provider
;
