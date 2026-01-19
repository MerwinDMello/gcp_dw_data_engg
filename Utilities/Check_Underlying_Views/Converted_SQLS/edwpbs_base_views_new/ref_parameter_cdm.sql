-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_parameter_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_parameter_cdm AS SELECT
    a.company_code,
    a.coid,
    a.parameter_group_code,
    a.code_type,
    a.code_value_text,
    a.code_detail_type,
    a.code_detail_text,
    a.eff_from_date,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ref_parameter AS a
;
