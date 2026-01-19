-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_discrepany_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discrepany_year AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.dim_discrepany_year
;
