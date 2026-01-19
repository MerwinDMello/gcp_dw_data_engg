-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_service_hcpcs_modifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_service_hcpcs_modifier
   OPTIONS(description='Table to maintain the Service Procedure Modifiers which identifies special circumstances related to the performance of the service')
  AS SELECT
      a.service_guid,
      a.hcpcs_type_ind,
      a.hcpcs_modifier_seq_num,
      a.hcpcs_modifier_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_hcpcs_modifier AS a
  ;
