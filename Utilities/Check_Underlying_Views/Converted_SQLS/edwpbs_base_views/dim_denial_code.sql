-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_denial_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_denial_code AS SELECT
    dim_denial_code.denial_code_sid,
    dim_denial_code.denial_code,
    dim_denial_code.denial_code_desc,
    dim_denial_code.denial_short_desc,
    dim_denial_code.denial_hier_name,
    dim_denial_code.dw_last_update_date_time,
    dim_denial_code.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_denial_code
;
