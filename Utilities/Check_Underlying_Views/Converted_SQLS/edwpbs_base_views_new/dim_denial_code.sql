-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
