-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_ar_dollar_stratification.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_ar_dollar_stratification AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_dollar_stratification
  WHERE upper(dim_dollar_stratification.dollar_strf_hier_name) = 'AR STRF HIERARCHY'
;
