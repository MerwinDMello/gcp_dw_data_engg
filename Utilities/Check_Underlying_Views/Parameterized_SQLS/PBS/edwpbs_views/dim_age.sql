-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_age.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_age AS SELECT
    dim_age.age_sid,
    dim_age.aso_bso_storage_code,
    dim_age.age_name_parent,
    dim_age.age_name_child,
    dim_age.age_alias_name,
    dim_age.alias_table_name,
    dim_age.sort_key_num,
    dim_age.two_pass_calc_code,
    dim_age.consolidation_code,
    dim_age.storage_code,
    dim_age.formula_text,
    dim_age.member_solve_order_num,
    dim_age.age_hier_name,
    dim_age.age_name_1_uda,
    dim_age.age_name_2_uda
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.dim_age
;
