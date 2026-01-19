-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_rarc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_rarc AS SELECT
    remittance_service_rarc.service_guid,
    remittance_service_rarc.rarc_qualifier_code,
    remittance_service_rarc.rarc_code,
    remittance_service_rarc.audit_date,
    remittance_service_rarc.delete_ind,
    remittance_service_rarc.delete_date,
    remittance_service_rarc.coid,
    remittance_service_rarc.company_code,
    remittance_service_rarc.dw_last_update_date_time,
    remittance_service_rarc.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.remittance_service_rarc
;
