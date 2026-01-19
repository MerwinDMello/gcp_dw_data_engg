-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_value.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_value
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Value')
  AS SELECT
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
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_value
  ;
