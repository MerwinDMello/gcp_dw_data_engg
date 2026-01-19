-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_remittance_secn_rendering_prov.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_remittance_secn_rendering_prov
   OPTIONS(description='Crosswalk table for Claim Records & Service Records with Secondary Rendering Provider Details')
  AS SELECT
      a.claim_guid,
      a.payment_guid,
      a.service_guid,
      a.secn_rendering_provider_id_line_num,
      ROUND(a.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_remittance_secn_rendering_prov AS a
  ;
