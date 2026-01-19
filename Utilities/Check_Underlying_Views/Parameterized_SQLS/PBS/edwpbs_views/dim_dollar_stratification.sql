-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_dollar_stratification.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_dollar_stratification AS SELECT
    dim_dollar_stratification.dollar_strf_sid,
    dim_dollar_stratification.aso_bso_storage_code,
    dim_dollar_stratification.dollar_strf_parent,
    dim_dollar_stratification.dollar_strf_child,
    dim_dollar_stratification.dollar_strf_alias_name,
    dim_dollar_stratification.sort_key_num,
    dim_dollar_stratification.two_pass_calc_code,
    dim_dollar_stratification.consolidation_code,
    dim_dollar_stratification.storage_code,
    dim_dollar_stratification.formula_text,
    dim_dollar_stratification.member_solve_order_num,
    dim_dollar_stratification.dollar_strf_hier_name,
    dim_dollar_stratification.dollar_strf_uda_name
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.dim_dollar_stratification
  WHERE upper(dim_dollar_stratification.dollar_strf_hier_name) = 'PARS STRF HIERARCHY'
;
