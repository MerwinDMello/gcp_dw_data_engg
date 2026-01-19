-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_registry_criteria.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_registry_criteria
   OPTIONS(description='Contains the Criteria Codes for each Registry inclusion')
  AS SELECT
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
