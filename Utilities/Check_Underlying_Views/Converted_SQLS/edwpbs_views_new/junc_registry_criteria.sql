-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_registry_criteria.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_registry_criteria AS SELECT
    a.registry_code,
    a.coid,
    a.criteria_type_code,
    a.criteria_icd_type_code,
    a.criteria_code,
    a.category_code,
    a.company_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_registry_criteria AS a
;
