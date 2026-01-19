-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider details of the claims sent.')
  AS SELECT
      a.remittance_rendering_provider_sid,
      a.serv_provider_enty_type_qualifier_code,
      a.rendering_provider_last_org_name,
      a.rendering_provider_first_name,
      a.rendering_provider_middle_name,
      a.rendering_provider_name_suffix,
      a.serv_provider_id_qualifier_code,
      a.rendering_provider_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_remittance_rendering_provider AS a
  ;
