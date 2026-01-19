-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_provider_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_provider_service
   OPTIONS(description='Reference table to maintain the Provider Service Identification Information of the Services recevied.')
  AS SELECT
      ROUND(ref_provider_service.remittance_provider_serv_sid, 0, 'ROUND_HALF_EVEN') AS remittance_provider_serv_sid,
      ref_provider_service.provider_serv_id_qlfr_code,
      ref_provider_service.provider_serv_id,
      ref_provider_service.source_system_code,
      ref_provider_service.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.ref_provider_service
  ;
