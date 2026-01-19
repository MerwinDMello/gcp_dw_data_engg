-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_hcpcs_modifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_hcpcs_modifier AS SELECT
    remittance_service_hcpcs_modifier.service_guid,
    remittance_service_hcpcs_modifier.hcpcs_type_ind,
    remittance_service_hcpcs_modifier.hcpcs_modifier_seq_num,
    remittance_service_hcpcs_modifier.hcpcs_modifier_code,
    remittance_service_hcpcs_modifier.source_system_code,
    remittance_service_hcpcs_modifier.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.remittance_service_hcpcs_modifier
;
