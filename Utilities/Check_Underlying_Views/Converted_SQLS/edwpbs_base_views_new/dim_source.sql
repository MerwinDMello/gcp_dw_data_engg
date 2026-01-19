-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_source.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_source AS SELECT
    dim_source.source_sid,
    dim_source.source_hier_name,
    dim_source.aso_bso_storage_code,
    dim_source.source_parent,
    dim_source.source_child,
    dim_source.source_alias_name,
    dim_source.sort_key_num,
    dim_source.two_pass_calc_code,
    dim_source.consolidation_code,
    dim_source.storage_code,
    dim_source.formula_text,
    dim_source.member_solve_order_num,
    dim_source.source_uda_name,
    dim_source.source_system_code,
    dim_source.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_source
;
