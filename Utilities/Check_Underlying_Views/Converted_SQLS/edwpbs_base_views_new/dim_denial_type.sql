-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_denial_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_denial_type AS SELECT
    dim_denial_type.denial_type_sid,
    dim_denial_type.aso_bso_storage_code,
    dim_denial_type.denial_type_parent,
    dim_denial_type.denial_type_child,
    dim_denial_type.denial_type_alias_name,
    dim_denial_type.alias_table_name,
    dim_denial_type.sort_key_num,
    dim_denial_type.two_pass_calc_code,
    dim_denial_type.consolidation_code,
    dim_denial_type.storage_code,
    dim_denial_type.formula_text,
    dim_denial_type.member_solve_order_num,
    dim_denial_type.denial_type_hier_name,
    dim_denial_type.denial_type_name_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_denial_type
;
