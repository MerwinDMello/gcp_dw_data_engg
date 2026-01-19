-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
