-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_discrepancy_mc_cost_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_discrepancy_mc_cost_year AS SELECT
    dim_discrepany_year.year_sid AS medicare_cost_year_sid,
    dim_discrepany_year.aso_bso_storage_code,
    dim_discrepany_year.year_name_parent AS medicare_cost_year_name_parent,
    dim_discrepany_year.year_name_child AS medicare_cost_year_name_child,
    dim_discrepany_year.year_alias_name AS medicare_cost_year_alias_name,
    dim_discrepany_year.alias_table_name,
    dim_discrepany_year.sort_key_num,
    dim_discrepany_year.two_pass_calc_code,
    dim_discrepany_year.consolidation_code,
    dim_discrepany_year.storage_code,
    dim_discrepany_year.formula_text,
    dim_discrepany_year.member_solve_order_num,
    dim_discrepany_year.year_hier_name AS medicare_cost_year_hier_name,
    dim_discrepany_year.year_name_1_uda AS medicare_cost_year_name_1_uda,
    dim_discrepany_year.year_name_2_uda AS medicare_cost_year_name_2_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discrepany_year
  WHERE upper(dim_discrepany_year.year_hier_name) = 'MEDICARE COST YEAR HIERARCHY'
;
