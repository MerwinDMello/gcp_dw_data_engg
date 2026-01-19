-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_provider_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_provider_service AS SELECT
    ref_provider_service.remittance_provider_serv_sid,
    ref_provider_service.provider_serv_id_qlfr_code,
    ref_provider_service.provider_serv_id,
    ref_provider_service.source_system_code,
    ref_provider_service.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_provider_service
;
