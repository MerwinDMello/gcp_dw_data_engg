-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_year
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Year')
  AS SELECT
      dim_bdgt_year.year_sid,
      dim_bdgt_year.year_hier_name,
      dim_bdgt_year.year_name_parent,
      dim_bdgt_year.year_name_child,
      dim_bdgt_year.year_alias_name,
      dim_bdgt_year.aso_bso_storage_code,
      dim_bdgt_year.sort_key_num,
      dim_bdgt_year.consolidation_code,
      dim_bdgt_year.storage_code,
      dim_bdgt_year.two_pass_calc_code,
      dim_bdgt_year.formula_text,
      dim_bdgt_year.member_solve_order_num,
      dim_bdgt_year.year_uda_name,
      dim_bdgt_year.source_system_code,
      dim_bdgt_year.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_year
  ;
