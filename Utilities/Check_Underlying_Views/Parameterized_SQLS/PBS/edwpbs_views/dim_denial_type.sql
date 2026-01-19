-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_denial_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_denial_type AS SELECT
    dim_denial_type.denial_type_sid,
    dim_denial_type.aso_bso_storage_code,
    dim_denial_type.denial_type_parent,
    dim_denial_type.denial_type_child,
    dim_denial_type.denial_type_alias_name,
    dim_denial_type.alias_table_name,
    dim_denial_type.sort_key_num,
    dim_denial_type.two_pass_calc_code,
    dim_denial_type.consolidation_code,
    dim_denial_type.storage_code,
    dim_denial_type.formula_text,
    dim_denial_type.member_solve_order_num,
    dim_denial_type.denial_type_hier_name,
    dim_denial_type.denial_type_name_uda
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.dim_denial_type
;
