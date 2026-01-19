-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_rarc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_rarc
   OPTIONS(description='This is the service level remarks associated with the Service.')
  AS SELECT
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
