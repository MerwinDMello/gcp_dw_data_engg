CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_discrepany_year AS SELECT
    dim_discrepany_year.year_sid,
    dim_discrepany_year.aso_bso_storage_code,
    dim_discrepany_year.year_name_parent,
    dim_discrepany_year.year_name_child,
    dim_discrepany_year.year_alias_name,
    dim_discrepany_year.alias_table_name,
    dim_discrepany_year.sort_key_num,
    dim_discrepany_year.two_pass_calc_code,
    dim_discrepany_year.consolidation_code,
    dim_discrepany_year.storage_code,
    dim_discrepany_year.formula_text,
    dim_discrepany_year.member_solve_order_num,
    dim_discrepany_year.year_hier_name,
    dim_discrepany_year.year_name_1_uda,
    dim_discrepany_year.year_name_2_uda
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_discrepany_year
;