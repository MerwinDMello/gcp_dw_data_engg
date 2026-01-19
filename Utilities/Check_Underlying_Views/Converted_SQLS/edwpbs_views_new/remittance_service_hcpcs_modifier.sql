-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_service_hcpcs_modifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_service_hcpcs_modifier AS SELECT
    a.service_guid,
    a.hcpcs_type_ind,
    a.hcpcs_modifier_seq_num,
    a.hcpcs_modifier_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_hcpcs_modifier AS a
;
