-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_department.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_department AS SELECT
    dim_department.department_sid,
    dim_department.department_hier_name,
    dim_department.department_name_parent,
    dim_department.department_name_child,
    dim_department.department_alias_name,
    dim_department.aso_bso_storage_code,
    dim_department.sort_key_num,
    dim_department.consolidation_code,
    dim_department.storage_code,
    dim_department.two_pass_calc_code,
    dim_department.formula_text,
    dim_department.member_solve_order_num,
    dim_department.department_uda_name,
    dim_department.source_system_code,
    dim_department.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_department
;
