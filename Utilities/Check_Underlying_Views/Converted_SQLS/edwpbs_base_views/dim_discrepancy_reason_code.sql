-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_discrepancy_reason_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discrepancy_reason_code AS SELECT
    ROUND(dim_discrepancy_reason_code.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
    dim_discrepancy_reason_code.aso_bso_storage_code,
    dim_discrepancy_reason_code.reason_code_parent,
    dim_discrepancy_reason_code.reason_code_child,
    dim_discrepancy_reason_code.reason_code_alias_name,
    dim_discrepancy_reason_code.sort_key_num,
    dim_discrepancy_reason_code.two_pass_calc_code,
    dim_discrepancy_reason_code.consolidation_code,
    dim_discrepancy_reason_code.storage_code,
    dim_discrepancy_reason_code.formula_text,
    dim_discrepancy_reason_code.member_solve_order_num,
    dim_discrepancy_reason_code.reason_code_hier_name,
    dim_discrepancy_reason_code.reason_code_uda_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_discrepancy_reason_code
;
