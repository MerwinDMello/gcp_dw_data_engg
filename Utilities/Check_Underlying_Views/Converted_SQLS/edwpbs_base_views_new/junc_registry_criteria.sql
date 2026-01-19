-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_registry_criteria.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_registry_criteria AS SELECT
    junc_registry_criteria.registry_code,
    junc_registry_criteria.coid,
    junc_registry_criteria.criteria_type_code,
    junc_registry_criteria.criteria_icd_type_code,
    junc_registry_criteria.criteria_code,
    junc_registry_criteria.category_code,
    junc_registry_criteria.company_code,
    junc_registry_criteria.source_system_code,
    junc_registry_criteria.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.junc_registry_criteria
;
