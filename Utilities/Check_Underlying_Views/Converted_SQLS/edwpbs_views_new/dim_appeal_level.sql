-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_appeal_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_appeal_level AS SELECT
    a.appeal_level_sid,
    a.aso_bso_storage_code,
    a.appeal_level_name_parent,
    a.appeal_level_name_child,
    a.appeal_level_alias_name,
    a.sort_key_num,
    a.two_pass_calc_code,
    a.consolidation_code,
    a.storage_code,
    a.formula_text,
    a.member_solve_order_num,
    a.appeal_level_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_level AS a
;
