-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_department.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_department
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels in Departments')
  AS SELECT
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
