-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_discrepancy_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_discrepancy_type AS SELECT
    dim_discrepancy_type.discrepancy_type_sid,
    dim_discrepancy_type.aso_bso_storage_code,
    dim_discrepancy_type.discrepancy_type_name_parent,
    dim_discrepancy_type.discrepancy_type_name_child,
    dim_discrepancy_type.discrepancy_type_alias_name,
    dim_discrepancy_type.alias_table_name,
    dim_discrepancy_type.sort_key_num,
    dim_discrepancy_type.two_pass_calc_code,
    dim_discrepancy_type.consolidation_code,
    dim_discrepancy_type.storage_code,
    dim_discrepancy_type.formula_text,
    dim_discrepancy_type.member_solve_order_num,
    dim_discrepancy_type.discrepancy_type_hier_name,
    dim_discrepancy_type.discrepancy_type_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discrepancy_type
;
