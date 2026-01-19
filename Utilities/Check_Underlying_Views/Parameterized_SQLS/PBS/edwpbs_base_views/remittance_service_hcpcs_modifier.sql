-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_hcpcs_modifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.remittance_service_hcpcs_modifier
   OPTIONS(description='Table to maintain the Service Procedure Modifiers which identifies special circumstances related to the performance of the service')
  AS SELECT
      remittance_service_hcpcs_modifier.service_guid,
      remittance_service_hcpcs_modifier.hcpcs_type_ind,
      remittance_service_hcpcs_modifier.hcpcs_modifier_seq_num,
      remittance_service_hcpcs_modifier.hcpcs_modifier_code,
      remittance_service_hcpcs_modifier.source_system_code,
      remittance_service_hcpcs_modifier.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.remittance_service_hcpcs_modifier
  ;
