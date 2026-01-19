-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_remittance_provider_serv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_remittance_provider_serv
   OPTIONS(description='Crosswalk table for Service Records with Provider Service Details')
  AS SELECT
      a.service_guid,
      a.provider_serv_id_line_num,
      ROUND(a.remittance_provider_serv_sid, 0, 'ROUND_HALF_EVEN') AS remittance_provider_serv_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_remittance_provider_serv AS a
  ;
