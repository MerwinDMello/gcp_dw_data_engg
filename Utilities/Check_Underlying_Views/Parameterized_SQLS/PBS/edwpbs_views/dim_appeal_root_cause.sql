-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_appeal_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.dim_appeal_root_cause AS SELECT
    ROUND(bv.appeal_root_cause_sid, 0, 'ROUND_HALF_EVEN') AS appeal_root_cause_sid,
    bv.aso_bso_storage_code,
    bv.appeal_root_cause_parent,
    bv.appeal_root_cause_child,
    bv.appeal_root_cause_alias_name,
    bv.sort_key_num,
    bv.two_pass_calc_code,
    bv.consolidation_code,
    bv.storage_code,
    bv.formula_text,
    bv.member_solve_order_num,
    bv.appeal_root_cause_hier_name,
    bv.appeal_root_cause_uda_name
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.dim_appeal_root_cause AS bv
;
