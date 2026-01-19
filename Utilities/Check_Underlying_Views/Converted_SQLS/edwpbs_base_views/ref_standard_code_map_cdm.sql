-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_standard_code_map_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_standard_code_map_cdm AS SELECT
    ref_standard_code_map.company_code,
    ref_standard_code_map.coid,
    ref_standard_code_map.clinical_system_module_code,
    ref_standard_code_map.source_system_original_code,
    ref_standard_code_map.standard_code_type_text,
    ref_standard_code_map.standard_code,
    ref_standard_code_map.source_term_text,
    ref_standard_code_map.target_term_text,
    ref_standard_code_map.qualifier_text,
    ref_standard_code_map.is_free_text_sw,
    ref_standard_code_map.is_restricted_sw,
    ref_standard_code_map.last_publish_date_time,
    ref_standard_code_map.source_system_code,
    ref_standard_code_map.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ref_standard_code_map
;
