-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_disp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_appeal_disp AS SELECT
    dim_appeal_disp.appeal_disp_sid,
    dim_appeal_disp.aso_bso_storage_code,
    dim_appeal_disp.appeal_disp_parent,
    dim_appeal_disp.appeal_disp_child,
    dim_appeal_disp.appeal_disp_alias_name,
    dim_appeal_disp.sort_key_num,
    dim_appeal_disp.two_pass_calc_code,
    dim_appeal_disp.consolidation_code,
    dim_appeal_disp.storage_code,
    dim_appeal_disp.formula_text,
    dim_appeal_disp.member_solve_order_num,
    dim_appeal_disp.appeal_disp_hier_name,
    dim_appeal_disp.appeal_disp_uda_name
  FROM
    {{ params.param_pbs_core_dataset_name }}.dim_appeal_disp
;
