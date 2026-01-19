-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_los.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_los AS SELECT
    a.los_sid,
    a.aso_bso_storage_code,
    a.los_name_parent,
    a.los_name_child,
    a.los_alias_name,
    a.alias_table_name,
    a.sort_key_num,
    a.two_pass_calc_code,
    a.consolidation_code,
    a.storage_code,
    a.formula_text,
    a.member_solve_order_num,
    a.los_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_los AS a
;
