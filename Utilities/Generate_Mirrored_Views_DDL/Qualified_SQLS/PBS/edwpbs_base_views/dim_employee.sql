-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_employee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_employee
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Employee')
  AS SELECT
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
      `hca-hin-curated-mirroring-td`.edwpbs.dim_employee
  ;
