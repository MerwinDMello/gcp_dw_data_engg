-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_provider AS SELECT
    gl_provider.provider_src_sys_key,
    gl_provider.pe_date,
    gl_provider.hcp_dw_id,
    gl_provider.budget_cc_ind,
    gl_provider.birth_date,
    gl_provider.first_name,
    gl_provider.last_name,
    gl_provider.middle_name,
    gl_provider.last_update_date,
    gl_provider.npi,
    gl_provider.upin,
    '***' AS social_security_num,
    gl_provider.count_provider_ind,
    gl_provider.last_update_user_id,
    gl_provider.original_contract_start_date,
    gl_provider.same_store_ind,
    gl_provider.data_source_code,
    gl_provider.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.gl_provider
;
