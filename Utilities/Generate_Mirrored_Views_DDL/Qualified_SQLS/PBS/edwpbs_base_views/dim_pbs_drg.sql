-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_pbs_drg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_pbs_drg
   OPTIONS(description='Dimension table to capture Diagnosis related groupin with Medical and Surgical Grouping')
  AS SELECT
      dim_pbs_drg.ms_drg_sid,
      dim_pbs_drg.aso_bso_storage_code,
      dim_pbs_drg.ms_drg_name_parent,
      dim_pbs_drg.ms_drg_name_child,
      dim_pbs_drg.ms_drg_child_alias_name,
      dim_pbs_drg.alias_table_name,
      dim_pbs_drg.sort_key_num,
      dim_pbs_drg.consolidation_code,
      dim_pbs_drg.storage_code,
      dim_pbs_drg.two_pass_calc_code,
      dim_pbs_drg.formula_text,
      dim_pbs_drg.ms_drg_med_surg_code,
      dim_pbs_drg.ms_drg_med_surg_name,
      dim_pbs_drg.ms_chois_prod_line_code,
      dim_pbs_drg.ms_chois_prod_line_desc,
      dim_pbs_drg.member_solve_order_num,
      dim_pbs_drg.ms_drg_hier_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_pbs_drg
  ;
