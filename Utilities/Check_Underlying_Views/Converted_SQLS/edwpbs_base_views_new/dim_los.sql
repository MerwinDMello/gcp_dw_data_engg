-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_los.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_los AS SELECT
    dim_los.los_sid,
    dim_los.aso_bso_storage_code,
    dim_los.los_name_parent,
    dim_los.los_name_child,
    dim_los.los_alias_name,
    dim_los.alias_table_name,
    dim_los.sort_key_num,
    dim_los.two_pass_calc_code,
    dim_los.consolidation_code,
    dim_los.storage_code,
    dim_los.formula_text,
    dim_los.member_solve_order_num,
    dim_los.los_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_los
;
