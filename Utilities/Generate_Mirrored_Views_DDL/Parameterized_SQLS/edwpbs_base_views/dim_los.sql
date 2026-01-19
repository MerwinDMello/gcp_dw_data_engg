-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_los.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_los AS SELECT
    dim_los.los_sid,
    dim_los.aso_bso_storage_code,
    dim_los.los_name_parent,
    dim_los.los_name_child,
    dim_los.los_alias_name,
    dim_los.alias_table_name,
    dim_los.sort_key_num,
    dim_los.two_pass_calc_code,
    dim_los.consolidation_code,
    dim_los.storage_code,
    dim_los.formula_text,
    dim_los.member_solve_order_num,
    dim_los.los_hier_name
  FROM
    {{ params.param_pbs_core_dataset_name }}.dim_los
;
