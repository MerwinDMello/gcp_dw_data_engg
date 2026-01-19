-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider details of the claims sent.')
  AS SELECT
      ref_remittance_rendering_provider.remittance_rendering_provider_sid,
      ref_remittance_rendering_provider.serv_provider_enty_type_qualifier_code,
      ref_remittance_rendering_provider.rendering_provider_last_org_name,
      ref_remittance_rendering_provider.rendering_provider_first_name,
      ref_remittance_rendering_provider.rendering_provider_middle_name,
      ref_remittance_rendering_provider.rendering_provider_name_suffix,
      ref_remittance_rendering_provider.serv_provider_id_qualifier_code,
      ref_remittance_rendering_provider.rendering_provider_id,
      ref_remittance_rendering_provider.source_system_code,
      ref_remittance_rendering_provider.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
  ;
