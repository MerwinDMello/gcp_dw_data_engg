-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
