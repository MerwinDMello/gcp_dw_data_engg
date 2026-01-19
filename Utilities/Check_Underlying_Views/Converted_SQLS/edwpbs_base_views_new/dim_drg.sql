-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_drg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_drg AS SELECT
    dim_drg.drg_sid,
    dim_drg.aso_bso_storage_code,
    dim_drg.drg_name_parent,
    dim_drg.drg_name_child,
    dim_drg.drg_child_alias_name,
    dim_drg.alias_table_name,
    dim_drg.sort_key_num,
    dim_drg.consolidation_code,
    dim_drg.storage_code,
    dim_drg.two_pass_calc_code,
    dim_drg.formula_text,
    dim_drg.drg_med_surg_code,
    dim_drg.drg_med_surg_name,
    dim_drg.chois_product_line_code,
    dim_drg.chois_product_line_desc,
    dim_drg.member_solve_order_num,
    dim_drg.drg_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.dim_drg
;
