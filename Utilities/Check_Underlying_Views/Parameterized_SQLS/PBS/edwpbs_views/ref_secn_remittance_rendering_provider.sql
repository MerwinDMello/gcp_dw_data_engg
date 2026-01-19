-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_secn_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_secn_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider Secondary Information of the Claims recevied.')
  AS SELECT
      ROUND(a.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      a.secn_rendering_provider_id_qlfr_code,
      a.secn_rendering_provider_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_secn_remittance_rendering_provider AS a
  ;
