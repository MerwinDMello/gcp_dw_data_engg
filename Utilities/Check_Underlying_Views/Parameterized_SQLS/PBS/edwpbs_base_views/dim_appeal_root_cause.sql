-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_appeal_root_cause AS SELECT
    ROUND(dim_appeal_root_cause.appeal_root_cause_sid, 0, 'ROUND_HALF_EVEN') AS appeal_root_cause_sid,
    dim_appeal_root_cause.aso_bso_storage_code,
    dim_appeal_root_cause.appeal_root_cause_parent,
    dim_appeal_root_cause.appeal_root_cause_child,
    dim_appeal_root_cause.appeal_root_cause_alias_name,
    dim_appeal_root_cause.sort_key_num,
    dim_appeal_root_cause.two_pass_calc_code,
    dim_appeal_root_cause.consolidation_code,
    dim_appeal_root_cause.storage_code,
    dim_appeal_root_cause.formula_text,
    dim_appeal_root_cause.member_solve_order_num,
    dim_appeal_root_cause.appeal_root_cause_hier_name,
    dim_appeal_root_cause.appeal_root_cause_uda_name
  FROM
    {{ params.param_pbs_core_dataset_name }}.dim_appeal_root_cause
;
