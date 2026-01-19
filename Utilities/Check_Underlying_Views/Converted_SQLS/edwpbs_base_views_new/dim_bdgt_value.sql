-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_value.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_bdgt_value AS SELECT
    dim_bdgt_value.value_sid,
    dim_bdgt_value.value_hier_name,
    dim_bdgt_value.value_name_parent,
    dim_bdgt_value.value_name_child,
    dim_bdgt_value.value_alias_name,
    dim_bdgt_value.aso_bso_storage_code,
    dim_bdgt_value.sort_key_num,
    dim_bdgt_value.consolidation_code,
    dim_bdgt_value.storage_code,
    dim_bdgt_value.two_pass_calc_code,
    dim_bdgt_value.formula_text,
    dim_bdgt_value.member_solve_order_num,
    dim_bdgt_value.value_uda_name,
    dim_bdgt_value.source_system_code,
    dim_bdgt_value.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_bdgt_value
;
