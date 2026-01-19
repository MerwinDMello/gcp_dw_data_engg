-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_rcm_shared_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_shared_service AS SELECT
    lu_rcm_shared_service.shared_service_code,
    lu_rcm_shared_service.service_type_name,
    lu_rcm_shared_service.shared_service_name,
    lu_rcm_shared_service.shared_service_short_name,
    lu_rcm_shared_service.pas_id_current,
    lu_rcm_shared_service.dw_last_update_date_time,
    lu_rcm_shared_service.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_rcm_shared_service
;
