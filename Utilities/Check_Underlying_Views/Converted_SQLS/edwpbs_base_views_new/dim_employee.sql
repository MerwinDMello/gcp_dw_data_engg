-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_employee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_employee AS SELECT
    dim_employee.employee_sid,
    dim_employee.employee_hier_name,
    dim_employee.employee_name_parent,
    dim_employee.employee_name_child,
    dim_employee.employee_alias_name,
    dim_employee.aso_bso_storage_code,
    dim_employee.sort_key_num,
    dim_employee.consolidation_code,
    dim_employee.storage_code,
    dim_employee.two_pass_calc_code,
    dim_employee.formula_text,
    dim_employee.member_solve_order_num,
    dim_employee.employee_uda_name,
    dim_employee.source_system_code,
    dim_employee.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_employee
;
