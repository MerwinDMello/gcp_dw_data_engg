-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_secn_rendering_prov.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.junc_remittance_secn_rendering_prov
   OPTIONS(description='Crosswalk table for Claim Records & Service Records with Secondary Rendering Provider Details')
  AS SELECT
      junc_remittance_secn_rendering_prov.claim_guid,
      junc_remittance_secn_rendering_prov.payment_guid,
      junc_remittance_secn_rendering_prov.service_guid,
      junc_remittance_secn_rendering_prov.secn_rendering_provider_id_line_num,
      ROUND(junc_remittance_secn_rendering_prov.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      junc_remittance_secn_rendering_prov.source_system_code,
      junc_remittance_secn_rendering_prov.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.junc_remittance_secn_rendering_prov
  ;
