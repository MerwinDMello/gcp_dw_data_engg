-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_pbs_drg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_pbs_drg
   OPTIONS(description='Dimension table to capture Diagnosis related groupin with Medical and Surgical Grouping')
  AS SELECT
      a.ms_drg_sid,
      a.aso_bso_storage_code,
      a.ms_drg_name_parent,
      a.ms_drg_name_child,
      a.ms_drg_child_alias_name,
      a.alias_table_name,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.ms_drg_med_surg_code,
      a.ms_drg_med_surg_name,
      a.ms_chois_prod_line_code,
      a.ms_chois_prod_line_desc,
      a.member_solve_order_num,
      a.ms_drg_hier_name
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.dim_pbs_drg AS a
  ;
