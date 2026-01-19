-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_practice.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_practice AS SELECT
    gl_practice.practice_src_sys_key,
    gl_practice.pe_date,
    gl_practice.development_type_src_sys_key,
    gl_practice.practice_manager_src_sys_key,
    gl_practice.development_type_name,
    gl_practice.address_1,
    gl_practice.address_2,
    gl_practice.city,
    gl_practice.fax,
    gl_practice.manager_first_name,
    gl_practice.manager_last_name,
    gl_practice.manager_middle_name,
    gl_practice.practice_name,
    gl_practice.phone,
    gl_practice.state,
    gl_practice.zip_code,
    gl_practice.zip_code_ext,
    gl_practice.data_source_code,
    gl_practice.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.gl_practice
;
