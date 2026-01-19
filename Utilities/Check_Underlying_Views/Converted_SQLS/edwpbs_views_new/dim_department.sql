-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_department.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_department AS SELECT
    a.department_sid,
    a.department_hier_name,
    a.department_name_parent,
    a.department_name_child,
    a.department_alias_name,
    a.aso_bso_storage_code,
    a.sort_key_num,
    a.consolidation_code,
    a.storage_code,
    a.two_pass_calc_code,
    a.formula_text,
    a.member_solve_order_num,
    a.department_uda_name,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_department AS a
;
